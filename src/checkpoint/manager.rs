use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use ssz::Decode;
use summit_types::{
    checkpoint::Checkpoint as SummitCheckpoint, consensus_state::ConsensusState,
    scheme::MultisigScheme, Block as SummitBlock, FinalizedHeader as SummitFinalizedHeader,
};

use crate::{
    checkpoint::{
        manifest::sha256_file, CheckpointExecutor, CheckpointMetadata, ExecutionIdentity,
        SnapshotManifest, SNAPSHOT_MANIFEST_FILE_NAME,
    },
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

        tracing::info!("Step 1/8: Copying MDBX database");
        let db_dest = checkpoint_path.join("db").join("mdbx.dat");
        self.executor.copy_mdbx_database(&self.db_path, &db_dest).await?;

        tracing::info!("Step 2/8: Copying static_files");
        let source_db_dir = self.db_path.parent().ok_or_else(|| {
            CheckpointerError::InvalidPath(
                "Could not determine parent directory of db_path".to_string(),
            )
        })?;
        self.executor.copy_static_files(source_db_dir, checkpoint_path).await?;

        tracing::info!("Step 3/8: Deleting lock file");
        self.executor.delete_lock_file(checkpoint_path).await?;

        tracing::info!(checkpoint_block, "Step 4/8: Unwinding copied database");
        self.executor.unwind_database(checkpoint_path, checkpoint_block).await?;

        tracing::info!("Step 5/8: Fetching and verifying Summit checkpoint");
        let summit_identity = if let Some(summit_client) = &self.rpc_client.summit {
            Some(
                self.write_summit_checkpoint(
                    summit_client,
                    checkpoint_path,
                    epoch,
                    checkpoint_block,
                    expected_checkpoint_digest,
                )
                .await?,
            )
        } else {
            tracing::debug!("Summit integration disabled, skipping Summit checkpoint data");
            None
        };

        tracing::info!("Step 6/8: Writing embedded metadata");
        let metadata = CheckpointMetadata::new(epoch, checkpoint_block);
        let metadata_path = checkpoint_path.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        tokio::fs::write(metadata_path, metadata_json).await?;

        tracing::info!("Step 7/8: Compressing checkpoint and cleaning up");
        self.executor.compress_and_cleanup(checkpoint_path, epoch).await?;

        if let Some(summit_identity) = summit_identity {
            tracing::info!("Step 8/8: Hashing archive and writing manifest");
            self.write_snapshot_manifest(
                checkpoint_path,
                epoch,
                metadata.timestamp,
                summit_identity,
            )
            .await?;
        } else {
            tracing::info!("Step 8/8: Summit disabled, skipping snapshot manifest");
        }

        Ok(())
    }

    async fn write_summit_checkpoint(
        &self,
        summit_client: &SummitRpcClient,
        checkpoint_path: &Path,
        epoch: u64,
        checkpoint_block: u64,
        expected_checkpoint_digest: Option<[u8; 32]>,
    ) -> Result<SummitSnapshotIdentity> {
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

        let summit_checkpoint_identity = decode_summit_checkpoint_identity(
            epoch,
            checkpoint_block,
            checkpoint_data.digest,
            &checkpoint_data.checkpoint,
            &checkpoint_data.last_block,
            &checkpoint_data.finalized_header,
        )?;
        let reth_block = self.rpc_client.reth.get_block(checkpoint_block).await?;
        let reth_identity = ExecutionIdentity {
            block_number: reth_block.block_number()?,
            block_hash: reth_block.block_hash()?,
            state_root: reth_block.state_root()?,
        };
        validate_checkpoint_execution_identity(
            epoch,
            checkpoint_block,
            summit_checkpoint_identity.execution_block_hash,
            reth_identity,
        )?;
        let summit_identity = SummitSnapshotIdentity {
            checkpoint_digest: summit_checkpoint_identity.checkpoint_digest,
            execution: reth_identity,
        };

        tracing::info!(
            epoch,
            digest = ?checkpoint_data.digest,
            execution_block = summit_identity.execution.block_number,
            execution_hash = %hex::encode(summit_identity.execution.block_hash),
            state_root = %hex::encode(summit_identity.execution.state_root),
            checkpoint_bytes = checkpoint_data.checkpoint.len(),
            last_block_bytes = checkpoint_data.last_block.len(),
            finalized_header_bytes = checkpoint_data.finalized_header.len(),
            "Verified Summit checkpoint state against Reth"
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
        Ok(summit_identity)
    }

    async fn write_snapshot_manifest(
        &self,
        checkpoint_path: &Path,
        epoch: u64,
        created_at: chrono::DateTime<chrono::Utc>,
        summit_identity: SummitSnapshotIdentity,
    ) -> Result<()> {
        let archive_path = checkpoint_path.join(format!("epoch_{epoch}.tar.gz"));
        let archive_size = tokio::fs::metadata(&archive_path).await?.len();
        let archive_sha256 = sha256_file(&archive_path).await?;
        let manifest = SnapshotManifest::new(
            epoch,
            summit_identity.checkpoint_digest,
            summit_identity.execution,
            archive_sha256,
            archive_size,
            created_at,
        );
        let manifest_json = serde_json::to_string_pretty(&manifest)?;
        tokio::fs::write(checkpoint_path.join(SNAPSHOT_MANIFEST_FILE_NAME), manifest_json).await?;

        tracing::info!(
            epoch,
            archive_size,
            archive_sha256 = %hex::encode(archive_sha256),
            "Snapshot manifest written"
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SummitCheckpointIdentity {
    checkpoint_digest: [u8; 32],
    execution_block_hash: [u8; 32],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SummitSnapshotIdentity {
    checkpoint_digest: [u8; 32],
    execution: ExecutionIdentity,
}

fn decode_summit_checkpoint_identity(
    epoch: u64,
    checkpoint_block: u64,
    response_digest: [u8; 32],
    checkpoint_bytes: &[u8],
    last_block_bytes: &[u8],
    finalized_header_bytes: &[u8],
) -> Result<SummitCheckpointIdentity> {
    let checkpoint = SummitCheckpoint::from_ssz_bytes(checkpoint_bytes).map_err(|error| {
        CheckpointerError::CheckpointExecution(format!(
            "Failed to decode Summit checkpoint for epoch {epoch}: {error:?}"
        ))
    })?;
    let checkpoint_digest: [u8; 32] = checkpoint.digest.as_ref().try_into().map_err(|_| {
        CheckpointerError::CheckpointExecution(format!(
            "Summit checkpoint for epoch {epoch} contained a non-32-byte digest"
        ))
    })?;
    if checkpoint_digest != response_digest {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit checkpoint digest mismatch for epoch {epoch}: RPC response {}, artifact {}",
            hex::encode(response_digest),
            hex::encode(checkpoint_digest)
        )));
    }

    let checkpoint_state = ConsensusState::try_from(&checkpoint).map_err(|error| {
        CheckpointerError::CheckpointExecution(format!(
            "Failed to decode Summit checkpoint state for epoch {epoch}: {error:?}"
        ))
    })?;
    let finalized_header = SummitFinalizedHeader::<MultisigScheme>::from_ssz_bytes(
        finalized_header_bytes,
    )
    .map_err(|error| {
        CheckpointerError::CheckpointExecution(format!(
            "Failed to decode Summit finalized_header for epoch {epoch}: {error:?}"
        ))
    })?;
    let header_checkpoint_digest: [u8; 32] =
        finalized_header.header().checkpoint_hash().as_ref().try_into().map_err(|_| {
            CheckpointerError::CheckpointExecution(format!(
                "Summit finalized header for epoch {epoch} contained a non-32-byte checkpoint hash"
            ))
        })?;
    if header_checkpoint_digest != checkpoint_digest {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit finalized-header checkpoint hash mismatch for epoch {epoch}: header {}, checkpoint {}",
            hex::encode(header_checkpoint_digest),
            hex::encode(checkpoint_digest)
        )));
    }

    let last_block = SummitBlock::from_ssz_bytes(last_block_bytes).map_err(|error| {
        CheckpointerError::CheckpointExecution(format!(
            "Failed to decode Summit last_block for epoch {epoch}: {error:?}"
        ))
    })?;
    let finalized_header_digest = finalized_header.header().get_digest();
    if last_block.digest() != finalized_header_digest {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit last block/finalized header digest mismatch for epoch {epoch}: block {}, header {}",
            hex::encode(last_block.digest().as_ref()),
            hex::encode(finalized_header_digest.as_ref())
        )));
    }
    if checkpoint_state.get_head_digest() != last_block.parent() {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit checkpoint state/last block parent mismatch for epoch {epoch}: checkpoint head {}, last block parent {}",
            hex::encode(checkpoint_state.get_head_digest().as_ref()),
            hex::encode(last_block.parent().as_ref())
        )));
    }
    validate_summit_checkpoint_position(
        epoch,
        checkpoint_block,
        checkpoint_state.get_epoch(),
        checkpoint_state.get_latest_height(),
        finalized_header.header().epoch(),
        finalized_header.header().height(),
        last_block.epoch(),
        last_block.height(),
    )?;

    // The checkpoint captures the penultimate Summit state. Its forkchoice head
    // binds the corresponding execution block. The separately returned
    // `last_block` is the terminal Summit block and must not be used as the Reth
    // unwind identity.
    Ok(SummitCheckpointIdentity {
        checkpoint_digest,
        execution_block_hash: checkpoint_state.get_forkchoice().head_block_hash.0,
    })
}

#[allow(clippy::too_many_arguments)]
fn validate_summit_checkpoint_position(
    epoch: u64,
    checkpoint_block: u64,
    checkpoint_state_epoch: u64,
    checkpoint_state_height: u64,
    finalized_header_epoch: u64,
    finalized_header_height: u64,
    last_block_epoch: u64,
    last_block_height: u64,
) -> Result<()> {
    let terminal_height = checkpoint_block.checked_add(1).ok_or_else(|| {
        CheckpointerError::CheckpointExecution(format!(
            "Checkpoint block overflow while validating epoch {epoch}: {checkpoint_block}"
        ))
    })?;

    if checkpoint_state_epoch != epoch || checkpoint_state_height != checkpoint_block {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit checkpoint state position mismatch for epoch {epoch}: expected epoch {epoch} at height {checkpoint_block}, received epoch {checkpoint_state_epoch} at height {checkpoint_state_height}"
        )));
    }
    if finalized_header_epoch != epoch || finalized_header_height != terminal_height {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit finalized header position mismatch for epoch {epoch}: expected epoch {epoch} at terminal height {terminal_height}, received epoch {finalized_header_epoch} at height {finalized_header_height}"
        )));
    }
    if last_block_epoch != epoch || last_block_height != terminal_height {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit last block position mismatch for epoch {epoch}: expected epoch {epoch} at terminal height {terminal_height}, received epoch {last_block_epoch} at height {last_block_height}"
        )));
    }

    Ok(())
}

fn validate_checkpoint_execution_identity(
    epoch: u64,
    checkpoint_block: u64,
    summit_checkpoint_block_hash: [u8; 32],
    reth: ExecutionIdentity,
) -> Result<()> {
    if reth.block_number != checkpoint_block {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Reth returned block {} while verifying checkpoint block {checkpoint_block} for epoch {epoch}",
            reth.block_number
        )));
    }
    if summit_checkpoint_block_hash != reth.block_hash {
        return Err(CheckpointerError::CheckpointExecution(format!(
            "Summit checkpoint/Reth block hash mismatch for epoch {epoch} at block {checkpoint_block}: Summit {}, Reth {}",
            hex::encode(summit_checkpoint_block_hash),
            hex::encode(reth.block_hash)
        )));
    }

    Ok(())
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
    use super::{
        state_and_archive_allow_skip, validate_checkpoint_digest,
        validate_checkpoint_execution_identity, validate_summit_checkpoint_position,
        ExecutionIdentity,
    };

    fn execution_identity() -> ExecutionIdentity {
        ExecutionIdentity { block_number: 42, block_hash: [0x11; 32], state_root: [0x22; 32] }
    }

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

    #[test]
    fn checkpoint_position_accepts_penultimate_state_and_terminal_artifacts() {
        assert!(validate_summit_checkpoint_position(7, 42, 7, 42, 7, 43, 7, 43).is_ok());
    }

    #[test]
    fn checkpoint_position_rejects_terminal_checkpoint_state() {
        assert!(validate_summit_checkpoint_position(7, 42, 7, 43, 7, 43, 7, 43).is_err());
    }

    #[test]
    fn checkpoint_position_rejects_wrong_terminal_height() {
        assert!(validate_summit_checkpoint_position(7, 42, 7, 42, 7, 42, 7, 43).is_err());
        assert!(validate_summit_checkpoint_position(7, 42, 7, 42, 7, 43, 7, 42).is_err());
    }

    #[test]
    fn summit_checkpoint_head_and_reth_execution_identity_must_match() {
        let identity = execution_identity();

        assert!(
            validate_checkpoint_execution_identity(7, 42, identity.block_hash, identity).is_ok()
        );
    }

    #[test]
    fn execution_identity_rejects_wrong_unwind_block() {
        let mut reth = execution_identity();
        reth.block_number = 41;

        assert!(validate_checkpoint_execution_identity(7, 42, [0x11; 32], reth).is_err());
    }

    #[test]
    fn execution_identity_rejects_block_hash_mismatch() {
        let mut reth = execution_identity();
        reth.block_hash = [0x33; 32];

        assert!(validate_checkpoint_execution_identity(7, 42, [0x11; 32], reth).is_err());
    }
}
