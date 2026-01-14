use thiserror::Error;

/// Custom error types for the checkpointer service
#[derive(Error, Debug)]
pub enum CheckpointerError {
    #[error("RPC error: {0}")]
    Rpc(#[from] jsonrpsee::core::ClientError),

    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    #[error("Checkpoint execution failed: {0}")]
    CheckpointExecution(String),

    #[error("State persistence error: {0}")]
    StatePersistence(#[from] std::io::Error),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Invalid path: {0}")]
    InvalidPath(String),

    #[error("MDBX copy failed: {0}")]
    MdbxCopyFailed(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Parse error: {0}")]
    Parse(String),
}

impl From<serde_json::Error> for CheckpointerError {
    fn from(err: serde_json::Error) -> Self {
        CheckpointerError::Serialization(err.to_string())
    }
}

impl From<serde_cbor::Error> for CheckpointerError {
    fn from(err: serde_cbor::Error) -> Self {
        CheckpointerError::Serialization(err.to_string())
    }
}

impl From<std::num::ParseIntError> for CheckpointerError {
    fn from(err: std::num::ParseIntError) -> Self {
        CheckpointerError::Parse(err.to_string())
    }
}

impl From<config::ConfigError> for CheckpointerError {
    fn from(err: config::ConfigError) -> Self {
        CheckpointerError::Config(err.to_string())
    }
}

/// Result type alias for checkpointer operations
pub type Result<T> = std::result::Result<T, CheckpointerError>;
