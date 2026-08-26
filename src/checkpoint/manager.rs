use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::{
    checkpoint::{CheckpointExecutor, CheckpointMetadata},
    config::{CheckpointConfig, Config},
    error::{CheckpointerError, Result},
    rpc::{RpcClient, SummitRpcClient},
    schedule::{fixed_interval_schedule, summit_checkpoint_schedule},
    state::StateTracker,
};

const FINALIZED_HEADER_CACHE_DIR: &str = ".summit-finalized-headers";

/// Checkpoint manager orchestrates checkpoint creation.
pub struct CheckpointManager {
    config: CheckpointConfig,
    db_path: PathBuf,
    executor: CheckpointExecutor,
    pub state_tracker: Arc<StateTracker>,
    rpc_client: RpcClient,
}

impl CheckpointManager {
    /// Create a new checkpoint manager.
    pub fn new(config: &Config, state_tracker: Arc<StateTracker>, rpc_client: RpcClient) -> Self {
        let executor = CheckpointExecutor::new(
            config.checkpoint.mdbx_copy_path.clone(),
            config.checkpoint.reth_path.clone(),
            config.checkpoint.compact,
        );

        Self {
            config: config.checkpoint.clone(),
            db_path: config.reth.db_path.clone(),
            executor,
            state_tracker,
            rpc_client,
        }
    }

    /// Create a checkpoint for the given epoch, unwound to `checkpoint_block`.
    pub async fn create_checkpoint(&self, epoch: u64, checkpoint_block: u64) -> Result<()> {
        self.create_checkpoint_with_expected_digest(epoch, checkpoint_block, None).await
    }

    pub(crate) async fn create_checkpoint_with_expected_digest(
        &self,
        epoch: u64,
        checkpoint_block: u64,
        expected_checkpoint_digest: Option<[u8; 32]>,
    ) -> Result<()> {
        let start = std::time::Instant::now();
        let checkpoint_name = format!("epoch_{}", epoch);
        let final_path = self.config.output_dir.join(&checkpoint_name);
        let working_path = self.config.output_dir.join(format!(".{checkpoint_name}.partial"));

        if final_path.exists() {
            if self.checkpoint_exists(epoch) {
                tracing::info!(
                    epoch,
                    path = ?final_path,
                    "Checkpoint archive already exists; syncing state tracker"
                );
                self.state_tracker.update_last_checkpoint(epoch, checkpoint_block).await?;
                return Ok(());
            }

            tracing::warn!(
                epoch,
                path = ?final_path,
                "Removing checkpoint output without a non-empty archive"
            );
            if final_path.is_dir() {
                tokio::fs::remove_dir_all(&final_path).await?;
            } else {
                tokio::fs::remove_file(&final_path).await?;
            }
        }
        if working_path.exists() {
            tracing::warn!(path = ?working_path, "Removing incomplete checkpoint attempt");
            tokio::fs::remove_dir_all(&working_path).await?;
        }

        tracing::info!(checkpoint_name, epoch, checkpoint_block, "Starting checkpoint");
        let result = self
            .create_checkpoint_at(
                &working_path,
                epoch,
                checkpoint_block,
                expected_checkpoint_digest,
            )
            .await;
        if let Err(error) = result {
            if let Err(cleanup_error) = tokio::fs::remove_dir_all(&working_path).await {
                tracing::warn!(
                    path = ?working_path,
                    %cleanup_error,
                    "Failed to remove incomplete checkpoint attempt"
                );
            }
            return Err(error);
        }

        tokio::fs::rename(&working_path, &final_path).await?;
        self.state_tracker.update_last_checkpoint(epoch, checkpoint_block).await?;

        // Cleanup old snapshots if retention limit is set
        if let Err(e) = self.cleanup_old_snapshots().await {
            tracing::warn!("Failed to cleanup old snapshots: {}", e);
        }

        tracing::info!(
            checkpoint_name,
            duration = ?start.elapsed(),
            output = ?final_path,
            "Checkpoint completed successfully"
        );
        Ok(())
    }

    async fn create_checkpoint_at(
        &self,
        checkpoint_path: &Path,
        epoch: u64,
        checkpoint_block: u64,
        expected_checkpoint_digest: Option<[u8; 32]>,
    ) -> Result<()> {
        tokio::fs::create_dir_all(checkpoint_path).await?;
        tracing::debug!(path = ?checkpoint_path, "Created checkpoint working directory");

        tracing::info!("Step 1/7: Copying MDBX database");
        let db_dest = checkpoint_path.join("db").join("mdbx.dat");
        self.executor.copy_mdbx_database(&self.db_path, &db_dest).await?;

        tracing::info!("Step 2/7: Copying static_files");
        let source_db_dir = self.db_path.parent().ok_or_else(|| {
            CheckpointerError::InvalidPath(
                "Could not determine parent directory of db_path".to_string(),
            )
        })?;
        self.executor.copy_static_files(source_db_dir, checkpoint_path).await?;

        tracing::info!("Step 3/7: Deleting lock file");
        self.executor.delete_lock_file(checkpoint_path).await?;

        tracing::info!(checkpoint_block, "Step 4/7: Unwinding copied database");
        self.executor.unwind_database(checkpoint_path, checkpoint_block).await?;

        tracing::info!("Step 5/7: Fetching Summit checkpoint and verification headers");
        if let Some(summit_client) = &self.rpc_client.summit {
            self.write_summit_checkpoint(
                summit_client,
                checkpoint_path,
                epoch,
                expected_checkpoint_digest,
            )
            .await?;
        } else {
            tracing::debug!("Summit integration disabled, skipping Summit checkpoint data");
        }

        tracing::info!("Step 6/7: Writing metadata");
        let metadata = CheckpointMetadata::new(epoch, checkpoint_block);
        let metadata_path = checkpoint_path.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        tokio::fs::write(metadata_path, metadata_json).await?;

        tracing::info!("Step 7/7: Compressing checkpoint and cleaning up");
        self.executor.compress_and_cleanup(checkpoint_path, epoch).await
    }

    async fn write_summit_checkpoint(
        &self,
        summit_client: &SummitRpcClient,
        checkpoint_path: &Path,
        epoch: u64,
        expected_checkpoint_digest: Option<[u8; 32]>,
    ) -> Result<()> {
        let checkpoint_data = summit_client.get_checkpoint(epoch).await?;
        validate_checkpoint_digest(epoch, expected_checkpoint_digest, checkpoint_data.digest)?;
        if checkpoint_data.checkpoint.is_empty()
            || checkpoint_data.last_block.is_empty()
            || checkpoint_data.finalized_header.is_empty()
        {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit returned incomplete checkpoint artifacts for epoch {}",
                epoch
            )));
        }

        tracing::info!(
            epoch,
            digest = ?checkpoint_data.digest,
            checkpoint_bytes = checkpoint_data.checkpoint.len(),
            last_block_bytes = checkpoint_data.last_block.len(),
            finalized_header_bytes = checkpoint_data.finalized_header.len(),
            "Received Summit checkpoint"
        );

        let cache_dir = self.config.output_dir.join(FINALIZED_HEADER_CACHE_DIR);
        let genesis_header = self.prepare_finalized_header_cache(summit_client, &cache_dir).await?;

        let summit_dir = checkpoint_path.join("summit_checkpoint");
        let finalized_headers_dir = summit_dir.join("finalized_headers");
        tokio::fs::create_dir_all(&finalized_headers_dir).await?;

        let mut fetched_headers = 1_usize;
        let mut reused_headers = 0_usize;

        for header_epoch in 0..=epoch {
            let finalized_header = if header_epoch == 0 {
                genesis_header.clone()
            } else if header_epoch == epoch {
                let response = summit_client.get_finalized_header(header_epoch).await?;
                fetched_headers += 1;
                response.finalized_header
            } else {
                let (finalized_header, cache_hit) = self
                    .get_or_fetch_finalized_header(summit_client, &cache_dir, header_epoch)
                    .await?;
                if cache_hit {
                    reused_headers += 1;
                } else {
                    fetched_headers += 1;
                }
                finalized_header
            };

            if header_epoch == epoch {
                if finalized_header != checkpoint_data.finalized_header {
                    return Err(CheckpointerError::CheckpointExecution(format!(
                        "Finalized header chain terminal for epoch {} does not match getCheckpoint",
                        epoch
                    )));
                }
                if header_epoch != 0 {
                    self.write_cached_finalized_header(&cache_dir, header_epoch, &finalized_header)
                        .await?;
                }
            }

            tokio::fs::write(
                finalized_headers_dir.join(header_epoch.to_string()),
                finalized_header,
            )
            .await?;
        }

        tokio::fs::write(summit_dir.join("checkpoint"), &checkpoint_data.checkpoint).await?;
        tokio::fs::write(summit_dir.join("last_block"), &checkpoint_data.last_block).await?;
        tokio::fs::write(summit_dir.join("finalized_header"), &checkpoint_data.finalized_header)
            .await?;

        tracing::info!(
            epoch,
            finalized_headers = epoch.saturating_add(1),
            fetched_headers,
            reused_headers,
            cache = ?cache_dir,
            "Summit checkpoint verification bundle written successfully"
        );
        Ok(())
    }

    async fn prepare_finalized_header_cache(
        &self,
        summit_client: &SummitRpcClient,
        cache_dir: &Path,
    ) -> Result<Vec<u8>> {
        // Always refresh epoch 0 so a cache created for another chain cannot be reused.
        let live_genesis_header = summit_client.get_finalized_header(0).await?.finalized_header;
        let cached_genesis_path = cache_dir.join("0");
        let cached_genesis = match tokio::fs::read(&cached_genesis_path).await {
            Ok(header) if !header.is_empty() => Some(header),
            Ok(_) => None,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error.into()),
        };

        if cached_genesis.as_deref() != Some(live_genesis_header.as_slice()) {
            if tokio::fs::try_exists(cache_dir).await? {
                tracing::warn!(
                    cache = ?cache_dir,
                    "Resetting finalized-header cache because epoch 0 does not match Summit"
                );
                tokio::fs::remove_dir_all(cache_dir).await?;
            }
            tokio::fs::create_dir_all(cache_dir).await?;
            self.write_cached_finalized_header(cache_dir, 0, &live_genesis_header).await?;
        }

        Ok(live_genesis_header)
    }

    async fn get_or_fetch_finalized_header(
        &self,
        summit_client: &SummitRpcClient,
        cache_dir: &Path,
        epoch: u64,
    ) -> Result<(Vec<u8>, bool)> {
        let cache_path = cache_dir.join(epoch.to_string());
        match tokio::fs::read(&cache_path).await {
            Ok(header) if !header.is_empty() => return Ok((header, true)),
            Ok(_) => {
                tracing::warn!(epoch, path = ?cache_path, "Ignoring empty cached finalized header");
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }

        let response = summit_client.get_finalized_header(epoch).await?;
        self.write_cached_finalized_header(cache_dir, epoch, &response.finalized_header).await?;
        Ok((response.finalized_header, false))
    }

    async fn write_cached_finalized_header(
        &self,
        cache_dir: &Path,
        epoch: u64,
        finalized_header: &[u8],
    ) -> Result<()> {
        tokio::fs::create_dir_all(cache_dir).await?;
        let cache_path = cache_dir.join(epoch.to_string());
        let partial_path = cache_dir.join(format!(".{epoch}.partial"));

        tokio::fs::write(&partial_path, finalized_header).await?;
        if let Err(error) = tokio::fs::rename(&partial_path, &cache_path).await {
            let _ = tokio::fs::remove_file(&partial_path).await;
            return Err(error.into());
        }

        Ok(())
    }

    /// Verify that the checkpoint tools are available.
    pub async fn verify_checkpoint_tools(&self) -> Result<()> {
        self.executor.verify_available().await
    }

    /// Remove oldest snapshot directories when count exceeds the configured max_snapshots limit.
    async fn cleanup_old_snapshots(&self) -> Result<()> {
        let max = match self.config.max_snapshots {
            Some(max) => max as usize,
            None => return Ok(()),
        };

        let mut entries = tokio::fs::read_dir(&self.config.output_dir).await?;
        let mut snapshots: Vec<(u64, PathBuf)> = Vec::new();

        while let Some(entry) = entries.next_entry().await? {
            let name = entry.file_name();
            let name_str = name.to_string_lossy();
            if let Some(epoch_str) = name_str.strip_prefix("epoch_") {
                if let Ok(epoch) = epoch_str.parse::<u64>() {
                    snapshots.push((epoch, entry.path()));
                }
            }
        }

        if snapshots.len() <= max {
            return Ok(());
        }

        snapshots.sort_by_key(|(epoch, _)| *epoch);
        let to_remove = snapshots.len() - max;

        for (epoch, path) in snapshots.into_iter().take(to_remove) {
            tracing::info!("Removing old snapshot: epoch_{}", epoch);
            tokio::fs::remove_dir_all(&path).await?;
        }

        Ok(())
    }

    /// Check whether a non-empty compressed checkpoint archive exists for the given epoch.
    pub fn checkpoint_exists(&self, epoch: u64) -> bool {
        let archive_path = self
            .config
            .output_dir
            .join(format!("epoch_{}", epoch))
            .join(format!("epoch_{}.tar.gz", epoch));

        std::fs::metadata(archive_path)
            .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
    }

    /// Whether state and disk agree that a candidate epoch has already been handled.
    pub(crate) fn checkpoint_is_satisfied(
        &self,
        epoch: u64,
        last_checkpointed_epoch: Option<u64>,
    ) -> bool {
        state_and_archive_allow_skip(epoch, last_checkpointed_epoch, self.checkpoint_exists(epoch))
    }

    /// Ensure a checkpoint exists for the latest finalized epoch. Intended to be called once
    /// at startup so we don't have to wait for the next epoch boundary to take a checkpoint.
    ///
    /// When summit is enabled, we read the latest checkpoint info and its historical epoch
    /// bounds via `getLatestCheckpointInfo` / `getEpochBounds`. When summit is disabled, we
    /// fall back to fixed-interval block math.
    pub async fn ensure_latest_checkpoint(&self) -> Result<()> {
        let (latest_epoch, checkpoint_block, ready_block, expected_checkpoint_digest) = match &self
            .rpc_client
            .summit
        {
            Some(summit) => {
                let Some(info) = summit.get_latest_checkpoint_info().await? else {
                    tracing::info!("Startup check: Summit has not produced a checkpoint yet");
                    return Ok(());
                };
                let bounds = summit.get_epoch_bounds(info.epoch).await?;
                let schedule =
                    summit_checkpoint_schedule(bounds, self.config.checkpoint_delay_blocks);
                (info.epoch, schedule.checkpoint_block, schedule.ready_block, Some(info.digest))
            }
            None => {
                let current_block = self.rpc_client.reth.get_block_number().await?;
                let current_epoch = current_block / self.config.epoch_blocks;
                if current_epoch == 0 {
                    tracing::info!(
                        "Startup check: current block {} is still in epoch 0, nothing to checkpoint",
                        current_block
                    );
                    return Ok(());
                }
                let schedule = fixed_interval_schedule(
                    current_epoch,
                    self.config.epoch_blocks,
                    self.config.checkpoint_delay_blocks,
                );
                // The previous epoch is the most recently finalized one.
                (current_epoch - 1, schedule.checkpoint_block, schedule.ready_block, None)
            }
        };

        let last_checkpointed_epoch = self.state_tracker.last_epoch().await;
        if self.checkpoint_is_satisfied(latest_epoch, last_checkpointed_epoch) {
            tracing::info!(
                latest_epoch,
                ?last_checkpointed_epoch,
                "Startup check: latest epoch already checkpointed, skipping"
            );
            return Ok(());
        }

        if last_checkpointed_epoch == Some(latest_epoch) {
            tracing::warn!(
                latest_epoch,
                "State records latest epoch, but its archive is missing; recreating it"
            );
        }

        if self.checkpoint_exists(latest_epoch) {
            tracing::info!(
                "Startup check: checkpoint archive for epoch {} already exists on disk, syncing state tracker",
                latest_epoch
            );
            self.state_tracker.update_last_checkpoint(latest_epoch, checkpoint_block).await?;
            return Ok(());
        }

        // Make sure reth has caught up at least to the ready block — otherwise we can't
        // safely snapshot yet.
        let current_reth_block = self.rpc_client.reth.get_block_number().await?;
        if current_reth_block < ready_block {
            tracing::info!(
                "Startup check: reth at block {} is behind ready block {} for epoch {}; deferring to monitor",
                current_reth_block,
                ready_block,
                latest_epoch
            );
            return Ok(());
        }

        tracing::info!(
            "Startup check: creating checkpoint for epoch {} unwound to block {} (reth at {})",
            latest_epoch,
            checkpoint_block,
            current_reth_block
        );
        self.create_checkpoint_with_expected_digest(
            latest_epoch,
            checkpoint_block,
            expected_checkpoint_digest,
        )
        .await?;

        Ok(())
    }
}

fn validate_checkpoint_digest(
    epoch: u64,
    expected_digest: Option<[u8; 32]>,
    actual_digest: [u8; 32],
) -> Result<()> {
    if let Some(expected_digest) = expected_digest {
        if actual_digest != expected_digest {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit checkpoint digest changed for epoch {epoch}: expected {expected_digest:?}, received {actual_digest:?}"
            )));
        }
    }

    Ok(())
}

fn state_and_archive_allow_skip(
    epoch: u64,
    last_checkpointed_epoch: Option<u64>,
    archive_exists: bool,
) -> bool {
    last_checkpointed_epoch.is_some_and(|last| last > epoch || (last == epoch && archive_exists))
}

#[cfg(test)]
mod tests {
    use super::{state_and_archive_allow_skip, validate_checkpoint_digest};

    #[test]
    fn checkpoint_skip_requires_archive_when_state_matches_epoch() {
        assert!(!state_and_archive_allow_skip(42, Some(42), false));
        assert!(state_and_archive_allow_skip(42, Some(42), true));
    }

    #[test]
    fn checkpoint_skip_preserves_state_that_is_ahead() {
        assert!(state_and_archive_allow_skip(42, Some(43), false));
        assert!(!state_and_archive_allow_skip(42, Some(41), true));
        assert!(!state_and_archive_allow_skip(42, None, true));
    }

    #[test]
    fn checkpoint_digest_must_match_discovery_response() {
        let digest = [42; 32];

        assert!(validate_checkpoint_digest(7, None, digest).is_ok());
        assert!(validate_checkpoint_digest(7, Some(digest), digest).is_ok());
        assert!(validate_checkpoint_digest(7, Some([41; 32]), digest).is_err());
    }
}
