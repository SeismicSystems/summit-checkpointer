use jsonrpsee::{core::RpcResult, proc_macros::rpc};

#[rpc(client, server)]
pub trait CheckpointerRpc {
    /// Health check endpoint that returns "OK" if service is running
    #[method(name = "healthCheck")]
    async fn health_check(&self) -> RpcResult<String>;

    /// Downloads a `.tar.gz` snapshot archive into the configured per-epoch directory.
    #[method(name = "snapshot.download_encrypted_snapshot")]
    async fn download_encrypted_snapshot(&self, epoch: u64, url: String) -> RpcResult<()>;

    /// Restores from an encrypted snapshot
    #[method(name = "snapshot.restore_from_encrypted_snapshot")]
    async fn restore_from_encrypted_snapshot(&self, epoch: u64) -> RpcResult<()>;

    /// Get an encrypted snapshot from this servers database
    #[method(name = "snapshot.get_encrypted_snapshot")]
    async fn get_encrypted_snapshot(&self, epoch: u64) -> RpcResult<Vec<u8>>;

    /// List all encrypted snapshots stored in this enclave
    #[method(name = "snapshot.list_all_encrypted_snapshots")]
    async fn list_all_encrypted_snapshots(&self) -> RpcResult<Vec<u64>>;

    /// List all encrypted snapshots stored in this enclave
    #[method(name = "snapshot.list_latest_stored_encrypted_snapshot")]
    async fn list_latest_encrypted_snapshots(&self) -> RpcResult<u64>;
}
