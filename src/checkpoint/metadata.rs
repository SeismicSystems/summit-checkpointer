use serde::{Deserialize, Serialize};

/// Metadata stored alongside each checkpoint
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    /// Epoch number for this checkpoint
    pub epoch: u64,

    /// Block to which the copied Reth database was unwound.
    pub block_number: u64,

    /// Timestamp when checkpoint was created
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl CheckpointMetadata {
    /// Create new checkpoint metadata
    pub fn new(epoch: u64, block_number: u64) -> Self {
        Self { epoch, block_number, timestamp: chrono::Utc::now() }
    }
}
