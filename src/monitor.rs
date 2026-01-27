use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    checkpoint::CheckpointManager,
    config::MonitorConfig,
    error::{CheckpointerError, Result},
    rpc::RpcClient,
};

/// Block monitor that watches for epoch boundaries and triggers checkpoints
pub struct BlockMonitor {
    rpc_client: RpcClient,
    checkpoint_manager: Arc<CheckpointManager>,
    config: MonitorConfig,
    epoch_blocks: u64,
    checkpoint_delay_blocks: u64,
}

impl BlockMonitor {
    /// Create a new block monitor
    pub fn new(
        rpc_client: RpcClient,
        checkpoint_manager: Arc<CheckpointManager>,
        config: MonitorConfig,
        epoch_blocks: u64,
        checkpoint_delay_blocks: u64,
    ) -> Self {
        Self { rpc_client, checkpoint_manager, config, epoch_blocks, checkpoint_delay_blocks }
    }

    /// Run the monitoring loop until cancelled
    pub async fn run_until_cancelled(&mut self, cancel_token: CancellationToken) -> Result<()> {
        tracing::info!(
            "Starting block monitor (epoch_blocks={}, checkpoint_delay_blocks={}, poll_interval={}s)",
            self.epoch_blocks,
            self.checkpoint_delay_blocks,
            self.config.poll_interval_secs
        );

        loop {
            tokio::select! {
                _ = cancel_token.cancelled() => {
                    tracing::info!("Block monitor shutting down");
                    break;
                }
                _ = tokio::time::sleep(Duration::from_secs(self.config.poll_interval_secs)) => {
                    if let Err(e) = self.check_and_checkpoint().await {
                        match e {
                            CheckpointerError::Rpc(ref rpc_err) => {
                                tracing::warn!("RPC connection error: {}. Will retry...", rpc_err);
                                // Backoff on RPC errors
                                tokio::time::sleep(Duration::from_secs(self.config.retry_interval_secs)).await;
                            }
                            CheckpointerError::CheckpointExecution(ref msg) => {
                                tracing::error!("Checkpoint execution failed: {}. Continuing monitoring...", msg);
                            }
                            _ => {
                                tracing::error!("Unexpected error in monitoring loop: {}", e);
                                // Backoff on unexpected errors
                                tokio::time::sleep(Duration::from_secs(self.config.retry_interval_secs)).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check current block and create checkpoint if conditions are met
    async fn check_and_checkpoint(&self) -> Result<()> {
        // Get current block number from reth
        let current_block = self.rpc_client.reth.get_block_number().await?;

        // Calculate current epoch
        let current_epoch = current_block / self.epoch_blocks;

        // Get last checkpointed epoch from state
        let last_checkpointed_epoch = self.checkpoint_manager.state_tracker.last_epoch().await;

        // Check if we have crossed an epoch boundary and waited long enough
        if current_epoch > last_checkpointed_epoch {
            // Calculate the block number at the epoch boundary we just crossed
            let epoch_block = current_epoch * self.epoch_blocks;

            // Check if we've waited long enough after the epoch boundary
            if current_block >= epoch_block + self.checkpoint_delay_blocks {
                tracing::info!(
                    "Epoch boundary crossed and delay satisfied: epoch {} at block {} (current block: {}, waited {} blocks)",
                    current_epoch,
                    epoch_block,
                    current_block,
                    current_block - epoch_block
                );

                // Create checkpoint for the epoch block, not the current block
                self.checkpoint_manager.create_checkpoint(current_epoch, epoch_block).await?;
            } else {
                let blocks_waited = current_block - epoch_block;
                let blocks_to_wait = self.checkpoint_delay_blocks - blocks_waited;

                tracing::debug!(
                    "Epoch boundary crossed at block {} (epoch {}), waiting for {} more blocks before checkpoint (current: {}/{})",
                    epoch_block,
                    current_epoch,
                    blocks_to_wait,
                    blocks_waited,
                    self.checkpoint_delay_blocks
                );
            }
        } else {
            let blocks_in_epoch = current_block % self.epoch_blocks;
            let blocks_until_next = self.epoch_blocks - blocks_in_epoch;

            tracing::debug!(
                "Current: epoch={}, block={}, progress={}/{}, blocks_until_epoch_boundary={}",
                current_epoch,
                current_block,
                blocks_in_epoch,
                self.epoch_blocks,
                blocks_until_next
            );
        }

        Ok(())
    }
}
