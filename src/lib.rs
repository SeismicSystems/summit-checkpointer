pub mod checkpoint;
pub mod config;
pub mod error;
pub mod monitor;
pub mod rpc;
pub mod server;
pub mod state;

// Re-export the RPC client trait for library users
pub use server::api::CheckpointerRpcClient;

// Re-export jsonrpsee client utilities for constructing a CheckpointerRpcClient
pub use jsonrpsee::{
    core::ClientError,
    http_client::{HttpClient, HttpClientBuilder},
};

// Re-export commonly used types
pub use checkpoint::CheckpointManager;
pub use config::{Cli, Config};
pub use error::{CheckpointerError, Result};
pub use monitor::BlockMonitor;
pub use rpc::RpcClient;
pub use server::server::RpcServer;
pub use state::StateTracker;
