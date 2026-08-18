use std::{sync::Arc, time::Duration};
use tokio_util::sync::CancellationToken;

use crate::{
    checkpoint::CheckpointManager,
    config::MonitorConfig,
    error::{CheckpointerError, Result},
    rpc::{RpcClient, SummitRpcClient},
    schedule::{fixed_interval_schedule, summit_checkpoint_schedule},
};

/// Block monitor that watches for completed Summit checkpoints or fixed Reth intervals.
pub struct BlockMonitor {
    rpc_client: RpcClient,
    checkpoint_manager: Arc<CheckpointManager>,
    config: MonitorConfig,
    epoch_blocks: u64,
    checkpoint_delay_blocks: u64,
}

impl BlockMonitor {
    /// Create a new block monitor.
    pub fn new(
        rpc_client: RpcClient,
        checkpoint_manager: Arc<CheckpointManager>,
        config: MonitorConfig,
        epoch_blocks: u64,
        checkpoint_delay_blocks: u64,
    ) -> Self {
        Self { rpc_client, checkpoint_manager, config, epoch_blocks, checkpoint_delay_blocks }
    }

    /// Run the monitoring loop until cancelled.
    pub async fn run_until_cancelled(&mut self, cancel_token: CancellationToken) -> Result<()> {
        if self.rpc_client.summit.is_some() {
            tracing::info!(
                checkpoint_delay_blocks = self.checkpoint_delay_blocks,
                poll_interval_secs = self.config.poll_interval_secs,
                "Starting Summit-driven checkpoint monitor"
            );
        } else {
            tracing::info!(
                epoch_blocks = self.epoch_blocks,
                checkpoint_delay_blocks = self.checkpoint_delay_blocks,
                poll_interval_secs = self.config.poll_interval_secs,
                "Starting fixed-interval Reth checkpoint monitor"
            );
        }

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
                                tokio::time::sleep(Duration::from_secs(self.config.retry_interval_secs)).await;
                            }
                            CheckpointerError::Http(ref http_err) => {
                                tracing::warn!("HTTP connection error: {}. Will retry...", http_err);
                                tokio::time::sleep(Duration::from_secs(self.config.retry_interval_secs)).await;
                            }
                            CheckpointerError::CheckpointExecution(ref msg) => {
                                tracing::error!("Checkpoint execution failed: {}. Continuing monitoring...", msg);
                            }
                            _ => {
                                tracing::error!("Unexpected error in monitoring loop: {}", e);
                                tokio::time::sleep(Duration::from_secs(self.config.retry_interval_secs)).await;
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check current state and create a checkpoint if one is ready.
    async fn check_and_checkpoint(&self) -> Result<()> {
        let current_block = self.rpc_client.reth.get_block_number().await?;
        let last_checkpointed_epoch = self.checkpoint_manager.state_tracker.last_epoch().await;

        if let Some(summit) = &self.rpc_client.summit {
            self.check_summit_checkpoint(summit, current_block, last_checkpointed_epoch).await
        } else {
            self.check_fixed_interval_checkpoint(current_block, last_checkpointed_epoch).await
        }
    }

    async fn check_summit_checkpoint(
        &self,
        summit: &SummitRpcClient,
        current_block: u64,
        last_checkpointed_epoch: Option<u64>,
    ) -> Result<()> {
        let Some(checkpoint_info) = summit.get_latest_checkpoint_info().await? else {
            tracing::debug!("Summit has not produced a checkpoint yet");
            return Ok(());
        };
        if self
            .checkpoint_manager
            .checkpoint_is_satisfied(checkpoint_info.epoch, last_checkpointed_epoch)
        {
            tracing::debug!(
                latest_summit_epoch = checkpoint_info.epoch,
                last_checkpointed_epoch,
                "No new Summit checkpoint"
            );
            return Ok(());
        }

        let bounds = summit.get_epoch_bounds(checkpoint_info.epoch).await?;
        let schedule = summit_checkpoint_schedule(bounds, self.checkpoint_delay_blocks);
        if current_block < schedule.ready_block {
            tracing::debug!(
                epoch = checkpoint_info.epoch,
                current_block,
                epoch_last_height = bounds.last_height,
                ready_block = schedule.ready_block,
                "Summit checkpoint exists; waiting for Reth checkpoint delay"
            );
            return Ok(());
        }

        tracing::info!(
            epoch = checkpoint_info.epoch,
            checkpoint_digest = ?checkpoint_info.digest,
            epoch_first_height = bounds.first_height,
            epoch_last_height = bounds.last_height,
            checkpoint_block = schedule.checkpoint_block,
            current_block,
            "New Summit checkpoint is ready for snapshotting"
        );
        self.checkpoint_manager
            .create_checkpoint_with_expected_digest(
                checkpoint_info.epoch,
                schedule.checkpoint_block,
                Some(checkpoint_info.digest),
            )
            .await
    }

    async fn check_fixed_interval_checkpoint(
        &self,
        current_block: u64,
        last_checkpointed_epoch: Option<u64>,
    ) -> Result<()> {
        let current_epoch = current_block / self.epoch_blocks;
        if current_epoch == 0 {
            return Ok(());
        }

        // We are currently in epoch N, so epoch N - 1 is the one that just completed.
        let completed_epoch = current_epoch - 1;
        if self.checkpoint_manager.checkpoint_is_satisfied(completed_epoch, last_checkpointed_epoch)
        {
            return Ok(());
        }

        // current_epoch identifies the boundary that ended completed_epoch.
        let schedule =
            fixed_interval_schedule(current_epoch, self.epoch_blocks, self.checkpoint_delay_blocks);
        if current_block < schedule.ready_block {
            tracing::debug!(
                completed_epoch,
                current_block,
                ready_block = schedule.ready_block,
                "Waiting for fixed-interval checkpoint delay"
            );
            return Ok(());
        }

        tracing::info!(
            epoch = completed_epoch,
            checkpoint_block = schedule.checkpoint_block,
            current_block,
            "Fixed-interval Reth checkpoint is ready"
        );
        self.checkpoint_manager.create_checkpoint(completed_epoch, schedule.checkpoint_block).await
    }
}
