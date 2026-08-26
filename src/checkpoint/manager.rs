use std::{path::PathBuf, sync::Arc};

use crate::{
    checkpoint::{CheckpointExecutor, CheckpointMetadata},
    config::{CheckpointConfig, Config},
    error::Result,
    rpc::RpcClient,
    state::StateTracker,
};

/// Checkpoint manager orchestrates checkpoint creation
pub struct CheckpointManager {
    config: CheckpointConfig,
    db_path: PathBuf,
    executor: CheckpointExecutor,
    pub state_tracker: Arc<StateTracker>,
    rpc_client: RpcClient,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
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

    /// Create a checkpoint for the given epoch.
    ///
    /// `block_number` is the height of the last block included in the finalized epoch
    /// (i.e. `summit_types::Block::from_ssz_bytes(checkpoint.last_block)?.header.height`).
    /// The reth db copy is unwound to `block_number - 1`, matching summit's finalized tip.
    pub async fn create_checkpoint(&self, epoch: u64, block_number: u64) -> Result<()> {
        let start = std::time::Instant::now();

        // Generate checkpoint directory name
        let checkpoint_name = format!("epoch_{}", epoch);
        let checkpoint_path = self.config.output_dir.join(&checkpoint_name);

        tracing::info!(
            "Starting checkpoint: {} (epoch={}, block={})",
            checkpoint_name,
            epoch,
            block_number
        );

        // Create checkpoint directory
        tokio::fs::create_dir_all(&checkpoint_path).await?;
        tracing::debug!("Created checkpoint directory: {:?}", checkpoint_path);

        // Step 1: Copy MDBX database
        tracing::info!("Step 1/7: Copying MDBX database");
        let db_dest = checkpoint_path.join("db").join("mdbx.dat");
        self.executor.copy_mdbx_database(&self.db_path, &db_dest).await?;

        // Step 2: Copy static_files directory
        tracing::info!("Step 2/7: Copying static_files");
        let source_db_dir = self.db_path.parent().ok_or_else(|| {
            crate::error::CheckpointerError::InvalidPath(
                "Could not determine parent directory of db_path".to_string(),
            )
        })?;
        self.executor.copy_static_files(source_db_dir, &checkpoint_path).await?;

        // Step 3: Delete lock file
        tracing::info!("Step 3/7: Deleting lock file");
        self.executor.delete_lock_file(&checkpoint_path).await?;

        // Step 4: Unwind database to one block before the summit-finalized tip of this epoch.
        let unwind_target = block_number.saturating_sub(1);
        tracing::info!("Step 4/7: Unwinding database to block {}", unwind_target);
        self.executor.unwind_database(&checkpoint_path, unwind_target).await?;

        // Step 5: Fetch and write Summit checkpoint data
        tracing::info!("Step 5/7: Fetching Summit checkpoint data");
        if let Some(summit_client) = &self.rpc_client.summit {
            // The `epoch` parameter is now the summit-authoritative epoch number (passed in
            // from the monitor / ensure_latest_checkpoint, which read it from
            // summit.getLatestEpoch). Fetch checkpoint data for that exact epoch.
            let summit_epoch = epoch;

            tracing::debug!("Fetching Summit checkpoint data for epoch {}", summit_epoch);

            match summit_client.get_checkpoint(summit_epoch).await {
                Ok(checkpoint_data) => {
                    tracing::info!(
                        "Received Summit checkpoint for epoch {}: digest={:?}, {} checkpoint bytes, {} last_block bytes, {} finalized_header bytes",
                        checkpoint_data.epoch,
                        &checkpoint_data.digest[..8],
                        checkpoint_data.checkpoint.len(),
                        checkpoint_data.last_block.len(),
                        checkpoint_data.finalized_header.len()
                    );

                    // Create summit_checkpoint directory
                    let summit_dir = checkpoint_path.join("summit_checkpoint");
                    tokio::fs::create_dir_all(&summit_dir).await?;
                    tracing::debug!("Created summit_checkpoint directory: {:?}", summit_dir);

                    // Write checkpoint bytes
                    let checkpoint_file = summit_dir.join("checkpoint");
                    tokio::fs::write(&checkpoint_file, &checkpoint_data.checkpoint).await?;
                    tracing::debug!(
                        "Wrote {} bytes to checkpoint file",
                        checkpoint_data.checkpoint.len()
                    );

                    // Write last_block bytes
                    let last_block_file = summit_dir.join("last_block");
                    tokio::fs::write(&last_block_file, &checkpoint_data.last_block).await?;
                    tracing::debug!(
                        "Wrote {} bytes to last_block file",
                        checkpoint_data.last_block.len()
                    );

                    // Write finalized_header bytes
                    let finalized_header_file = summit_dir.join("finalized_header");
                    tokio::fs::write(&finalized_header_file, &checkpoint_data.finalized_header)
                        .await?;
                    tracing::debug!(
                        "Wrote {} bytes to finalized_header file",
                        checkpoint_data.finalized_header.len()
                    );

                    tracing::info!("Summit checkpoint data written successfully");
                }
                Err(e) => {
                    tracing::warn!("Failed to fetch Summit checkpoint data: {}", e);
                    tracing::warn!("Continuing without Summit checkpoint data");
                }
            }
        } else {
            tracing::debug!("Summit integration disabled, skipping Summit checkpoint data");
        }

        // Step 6: Write metadata
        tracing::info!("Step 6/7: Writing metadata");
        let metadata = CheckpointMetadata::new(epoch, block_number);
        let metadata_path = checkpoint_path.join("metadata.json");
        let metadata_json = serde_json::to_string_pretty(&metadata)?;
        tokio::fs::write(metadata_path, metadata_json).await?;

        // Step 7: Compress and cleanup
        tracing::info!("Step 7/7: Compressing checkpoint and cleaning up");
        self.executor.compress_and_cleanup(&checkpoint_path, epoch).await?;

        // Update state tracker
        self.state_tracker.update_last_checkpoint(epoch, block_number).await?;

        // Cleanup old snapshots if retention limit is set
        if let Err(e) = self.cleanup_old_snapshots().await {
            tracing::warn!("Failed to cleanup old snapshots: {}", e);
        }

        let duration = start.elapsed();
        tracing::info!(
            "Checkpoint completed successfully: {} (took {:?})",
            checkpoint_name,
            duration
        );

        Ok(())
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

    /// Verify that the checkpoint tools are available
    pub async fn verify_checkpoint_tools(&self) -> Result<()> {
        self.executor.verify_available().await
    }

    /// Check whether a compressed checkpoint archive already exists on disk for the given epoch.
    pub fn checkpoint_exists(&self, epoch: u64) -> bool {
        self.config
            .output_dir
            .join(format!("epoch_{}", epoch))
            .join(format!("epoch_{}.tar.gz", epoch))
            .exists()
    }

    /// Ensure a checkpoint exists for the latest finalized epoch. Intended to be called once
    /// at startup so we don't have to wait for the next epoch boundary to take a checkpoint.
    ///
    /// When summit is enabled (the authoritative path), we call `getLatestCheckpoint` and
    /// SSZ-decode the returned `last_block` to learn the exact tip of the finalized epoch.
    /// The unwind inside `create_checkpoint` then lands at `height - 1`.
    ///
    /// If summit is disabled, we fall back to block-math (which is incorrect after an
    /// epoch-length change but preserves legacy behavior for fixed-length chains).
    pub async fn ensure_latest_checkpoint(&self) -> Result<()> {
        let (latest_epoch, last_block_height) = match &self.rpc_client.summit {
            Some(summit) => summit.get_latest_epoch_last_block().await?,
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
                // Previous epoch is the most recently finalized; its last block is the
                // block immediately before the current epoch's first block.
                (current_epoch - 1, current_epoch * self.config.epoch_blocks - 1)
            }
        };

        let last_checkpointed_epoch = self.state_tracker.last_epoch().await;
        if latest_epoch <= last_checkpointed_epoch {
            tracing::info!(
                "Startup check: latest epoch {} already checkpointed (last_epoch={}), skipping",
                latest_epoch,
                last_checkpointed_epoch
            );
            return Ok(());
        }

        if self.checkpoint_exists(latest_epoch) {
            tracing::info!(
                "Startup check: checkpoint archive for epoch {} already exists on disk, syncing state tracker",
                latest_epoch
            );
            self.state_tracker.update_last_checkpoint(latest_epoch, last_block_height).await?;
            return Ok(());
        }

        // Make sure reth has caught up at least to the epoch's final block — otherwise
        // we can't unwind to it.
        let current_reth_block = self.rpc_client.reth.get_block_number().await?;
        if current_reth_block < last_block_height {
            tracing::info!(
                "Startup check: reth at block {} is behind summit epoch {} tip (block {}); deferring to monitor",
                current_reth_block,
                latest_epoch,
                last_block_height
            );
            return Ok(());
        }

        tracing::info!(
            "Startup check: creating checkpoint for epoch {} at finalized block {} (reth at {})",
            latest_epoch,
            last_block_height,
            current_reth_block
        );
        self.create_checkpoint(latest_epoch, last_block_height).await?;

        Ok(())
    }
}
