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

    /// Create a checkpoint for the given epoch and block
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

        // Step 4: Unwind database to epoch_block - 2
        let unwind_target = block_number.saturating_sub(2);
        tracing::info!("Step 4/7: Unwinding database to block {}", unwind_target);
        self.executor.unwind_database(&checkpoint_path, unwind_target).await?;

        // Step 5: Fetch and write Summit checkpoint data
        tracing::info!("Step 5/7: Fetching Summit checkpoint data");
        if let Some(summit_client) = &self.rpc_client.summit {
            // Calculate Summit epoch: (block_number / epoch_blocks) - 1
            // Epochs start at 0, so block 200 is epoch 0, block 400 is epoch 1, etc.
            let summit_epoch = (block_number / self.config.epoch_blocks).saturating_sub(1);

            tracing::debug!(
                "Calculated Summit epoch: {} (block {} / epoch_blocks {} - 1)",
                summit_epoch,
                block_number,
                self.config.epoch_blocks
            );

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

    /// Ensure a checkpoint exists for the latest completed epoch. Intended to be called once
    /// at startup so we don't have to wait for the next epoch boundary to take a checkpoint.
    ///
    /// - If the state tracker or disk already shows a checkpoint for the latest epoch, skip.
    /// - If the latest epoch boundary was crossed but the configured delay hasn't elapsed yet, skip
    ///   and let the block monitor handle it on its next poll.
    /// - Otherwise, create the checkpoint immediately.
    pub async fn ensure_latest_checkpoint(&self) -> Result<()> {
        let epoch_blocks = self.config.epoch_blocks;
        let checkpoint_delay_blocks = self.config.checkpoint_delay_blocks;

        let current_block = self.rpc_client.reth.get_block_number().await?;
        let current_epoch = current_block / epoch_blocks;

        if current_epoch == 0 {
            tracing::info!(
                "Startup check: current block {} is still in epoch 0, no prior epoch to checkpoint",
                current_block
            );
            return Ok(());
        }

        let last_checkpointed_epoch = self.state_tracker.last_epoch().await;
        if current_epoch <= last_checkpointed_epoch {
            tracing::info!(
                "Startup check: latest epoch {} already checkpointed (last_epoch={}), skipping",
                current_epoch,
                last_checkpointed_epoch
            );
            return Ok(());
        }

        let epoch_block = current_epoch * epoch_blocks;

        if self.checkpoint_exists(current_epoch) {
            tracing::info!(
                "Startup check: checkpoint archive for epoch {} already exists on disk, syncing state tracker",
                current_epoch
            );
            self.state_tracker.update_last_checkpoint(current_epoch, epoch_block).await?;
            return Ok(());
        }

        if current_block < epoch_block + checkpoint_delay_blocks {
            let blocks_waited = current_block - epoch_block;
            tracing::info!(
                "Startup check: epoch {} boundary crossed at block {} but delay not satisfied ({}/{}); deferring to block monitor",
                current_epoch,
                epoch_block,
                blocks_waited,
                checkpoint_delay_blocks
            );
            return Ok(());
        }

        tracing::info!(
            "Startup check: creating checkpoint for latest completed epoch {} at block {} (current block: {})",
            current_epoch,
            epoch_block,
            current_block
        );
        self.create_checkpoint(current_epoch, epoch_block).await?;

        Ok(())
    }
}
