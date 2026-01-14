use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::error::Result;

/// State structure for tracking checkpoint progress
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointerState {
    pub last_checkpointed_epoch: u64,
    pub last_checkpoint_block: u64,
    pub last_update: chrono::DateTime<chrono::Utc>,
}

impl Default for CheckpointerState {
    fn default() -> Self {
        Self {
            last_checkpointed_epoch: 0,
            last_checkpoint_block: 0,
            last_update: chrono::Utc::now(),
        }
    }
}

/// State tracker with persistence
pub struct StateTracker {
    state: Arc<RwLock<CheckpointerState>>,
    path: std::path::PathBuf,
}

impl StateTracker {
    /// Load state from disk or create new state
    pub async fn load(path: &Path) -> Result<Self> {
        let state = if path.exists() {
            tracing::info!("Loading existing state from {:?}", path);
            let bytes = tokio::fs::read(path).await?;
            serde_cbor::from_slice(&bytes)?
        } else {
            tracing::info!("No existing state found, starting fresh");
            CheckpointerState::default()
        };

        tracing::info!(
            "State loaded: last_epoch={}, last_block={}",
            state.last_checkpointed_epoch,
            state.last_checkpoint_block
        );

        Ok(Self {
            state: Arc::new(RwLock::new(state)),
            path: path.to_path_buf(),
        })
    }

    /// Get last checkpointed epoch
    pub async fn last_epoch(&self) -> u64 {
        self.state.read().await.last_checkpointed_epoch
    }

    /// Get last checkpointed block
    pub async fn last_block(&self) -> u64 {
        self.state.read().await.last_checkpoint_block
    }

    /// Update last checkpointed epoch and block
    pub async fn update_last_checkpoint(&self, epoch: u64, block: u64) -> Result<()> {
        let mut state = self.state.write().await;
        state.last_checkpointed_epoch = epoch;
        state.last_checkpoint_block = block;
        state.last_update = chrono::Utc::now();

        // Persist to disk immediately
        self.save_internal(&state).await?;

        tracing::debug!(
            "State updated: epoch={}, block={}",
            epoch,
            block
        );

        Ok(())
    }

    /// Internal save function (requires state lock to be held)
    async fn save_internal(&self, state: &CheckpointerState) -> Result<()> {
        let bytes = serde_cbor::to_vec(state)?;

        // Create parent directory if it doesn't exist
        if let Some(parent) = self.path.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        tokio::fs::write(&self.path, bytes).await?;
        Ok(())
    }

    /// Explicit save for shutdown (uses read lock since no mutation needed)
    pub async fn save(&self) -> Result<()> {
        let state = self.state.read().await;
        self.save_internal(&state).await?;
        tracing::info!("State saved to {:?}", self.path);
        Ok(())
    }
}
