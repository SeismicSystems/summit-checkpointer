use std::{fs, net::SocketAddr, path::Path};

use jsonrpsee::{
    core::{async_trait, RpcResult},
    server::ServerBuilder,
    types::{ErrorCode, ErrorObjectOwned},
};
use tokio::io::AsyncWriteExt as _;
use tracing::info;

use crate::server::api::CheckpointerRpcServer;

pub const SNAPSHOT_FILE_PREFIX: &str = "epoch_";
pub const DATA_DISK_DIR: &str = "/home/ubuntu/checkpoints";

pub struct RpcServer;

impl RpcServer {
    pub fn new() -> Self {
        Self
    }

    pub async fn start_server(self, addr: SocketAddr) {
        let server = ServerBuilder::default()
            .build(addr)
            .await
            .expect("Failed to start rpc");

        let handle = server.start(self.into_rpc());

        info!("JSON-RPC Server started at {}", addr);

        handle.stopped().await;
    }
}

#[async_trait]
impl CheckpointerRpcServer for RpcServer {
    /// Health check endpoint that returns "OK" if service is running
    async fn health_check(&self) -> RpcResult<String> {
        Ok("OK".to_string())
    }

    /// Prepares an encrypted snapshot
    async fn download_encrypted_snapshot(&self, epoch: u64, url: String) -> RpcResult<()> {
        // Download the file
        let response = reqwest::get(&url)
            .await
            .map_err(|e| string_to_rpc_error(format!("Failed to download snapshot: {}", e)))?;

        if !response.status().is_success() {
            return Err(string_to_rpc_error(format!(
                "HTTP error: {}",
                response.status()
            )));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| string_to_rpc_error(format!("Failed to read response body: {}", e)))?;

        // Create the filename
        let filename = format!("{SNAPSHOT_FILE_PREFIX}{epoch}.tar.lz4");

        // Write to file
        let mut file = tokio::fs::File::create(format!("{DATA_DISK_DIR}/{filename}"))
            .await
            .map_err(|e| {
                string_to_rpc_error(format!("Failed to create file {}: {}", filename, e))
            })?;

        file.write_all(&bytes).await.map_err(|e| {
            string_to_rpc_error(format!("Failed to write to file {}: {}", filename, e))
        })?;

        Ok(())
    }

    /// Restores from an encrypted snapshot
    async fn restore_from_encrypted_snapshot(&self, epoch: u64) -> RpcResult<()> {
        todo!()
    }

    /// Get an encrypted snapshot from this servers database
    async fn get_encrypted_snapshot(&self, epoch: u64) -> RpcResult<Vec<u8>> {
        let snapshot_path = format!(
            "{DATA_DISK_DIR}/{SNAPSHOT_FILE_PREFIX}{epoch}/{SNAPSHOT_FILE_PREFIX}{epoch}.tar.lz4",
        );

        if !fs::exists(&snapshot_path).unwrap_or_default() {
            return Err(string_to_rpc_error(format!(
                "No snapshot for epoch {epoch} stored"
            )));
        }

        fs::read(snapshot_path).map_err(|e| {
            string_to_rpc_error(format!(
                "Failed to read snapshot for epoch {}: {}",
                epoch, e
            ))
        })
    }

    /// List all encrypted snapshots stored in this enclave
    async fn list_all_encrypted_snapshots(&self) -> RpcResult<Vec<u64>> {
        let dir_path = Path::new(DATA_DISK_DIR);

        let entries = fs::read_dir(dir_path).map_err(|e| {
            string_to_rpc_error(format!("Failed to read snapshots directory: {}", e))
        })?;

        let mut epochs = Vec::new();
        let prefix = format!("{SNAPSHOT_FILE_PREFIX}");

        for entry in entries {
            let entry = entry.map_err(|e| {
                string_to_rpc_error(format!("Failed to read directory entry: {}", e))
            })?;

            if let Some(filename) = entry.file_name().to_str() {
                if filename.starts_with(&prefix) {
                    // Extract epoch from filename
                    let epoch_str = filename.strip_prefix(&prefix);

                    if let Some(epoch_str) = epoch_str {
                        if let Ok(epoch) = epoch_str.parse::<u64>() {
                            epochs.push(epoch);
                        }
                    }
                }
            }
        }

        epochs.sort_unstable();
        Ok(epochs)
    }

    /// List all encrypted snapshots stored in this enclave
    async fn list_latest_encrypted_snapshots(&self) -> RpcResult<u64> {
        let all_snapshots = self.list_all_encrypted_snapshots().await?;

        all_snapshots
            .into_iter()
            .max()
            .ok_or_else(|| string_to_rpc_error("No snapshots found".to_string()))
    }
}

pub fn string_to_rpc_error(e: String) -> ErrorObjectOwned {
    ErrorObjectOwned::owned(ErrorCode::InternalError.code(), e, None::<()>)
}
