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
        // Summit is authoritative for both the epoch number and the finalized tip block.
        // Fall back to block-math only when summit is disabled (broken after an
        // epoch-length change, but preserves legacy behavior for fixed-length chains).
        let (latest_epoch, last_block_height) = match &self.rpc_client.summit {
            Some(summit) => summit.get_latest_epoch_last_block().await?,
            None => {
                let current_block = self.rpc_client.reth.get_block_number().await?;
                let current_epoch = current_block / self.epoch_blocks;
                if current_epoch == 0 {
                    return Ok(());
                }
                let last_block_of_finalized = current_epoch * self.epoch_blocks - 1;
                // Keep the configured delay in the fallback path only — summit-based
                // gating already implies finality.
                if current_block < last_block_of_finalized + 1 + self.checkpoint_delay_blocks {
                    return Ok(());
                }
                (current_epoch - 1, last_block_of_finalized)
            }
        };

        let last_checkpointed_epoch = self.checkpoint_manager.state_tracker.last_epoch().await;

        if latest_epoch <= last_checkpointed_epoch {
            tracing::debug!(
                "No new epoch: latest={}, last_checkpointed={}",
                latest_epoch,
                last_checkpointed_epoch
            );
            return Ok(());
        }

        // Reth must be caught up to the epoch's final block before we can unwind to it.
        let current_reth_block = self.rpc_client.reth.get_block_number().await?;
        if current_reth_block < last_block_height {
            tracing::debug!(
                "Reth at block {} is behind summit epoch {} tip ({}); waiting",
                current_reth_block,
                latest_epoch,
                last_block_height
            );
            return Ok(());
        }

        tracing::info!(
            "New epoch detected (epoch={}, prev_checkpointed={}); creating checkpoint at finalized block {} (reth at {})",
            latest_epoch,
            last_checkpointed_epoch,
            last_block_height,
            current_reth_block
        );

        self.checkpoint_manager.create_checkpoint(latest_epoch, last_block_height).await?;

        Ok(())
    }
}
