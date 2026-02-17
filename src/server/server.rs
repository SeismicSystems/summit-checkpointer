use std::{convert::Infallible, fs, net::SocketAddr, path::Path};

use futures_util::StreamExt;
use http_body::Frame;
use http_body_util::StreamBody;
use hyper::body::Incoming;
use jsonrpsee::{
    core::{async_trait, RpcResult},
    server::{serve_with_graceful_shutdown, stop_channel, HttpBody, ServerBuilder, ServerHandle},
    types::{ErrorCode, ErrorObjectOwned},
};
use tokio::{io::AsyncWriteExt as _, net::TcpListener};
use tokio_util::io::ReaderStream;
use tower::Service;
use tracing::info;

use crate::server::api::CheckpointerRpcServer;

pub const SNAPSHOT_FILE_PREFIX: &str = "epoch_";
pub const DATA_DISK_DIR: &str = "/persistent/checkpoints";

pub struct RpcServer;

impl RpcServer {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self
    }

    pub async fn start_server(self, addr: SocketAddr) -> ServerHandle {
        let listener = TcpListener::bind(addr).await.expect("Failed to bind RPC server");
        let (stop_handle, server_handle) = stop_channel();

        let rpc_module = self.into_rpc();
        let svc_builder = ServerBuilder::default().to_service_builder();

        tokio::spawn(async move {
            loop {
                let sock = tokio::select! {
                    res = listener.accept() => {
                        match res {
                            Ok((stream, _)) => stream,
                            Err(e) => {
                                tracing::error!("TCP accept error: {e}");
                                continue;
                            }
                        }
                    }
                    _ = stop_handle.clone().shutdown() => break,
                };

                let rpc_module = rpc_module.clone();
                let svc_builder = svc_builder.clone();
                let conn_stop = stop_handle.clone();
                let shutdown_stop = stop_handle.clone();

                let svc = tower::service_fn(move |req: http::Request<Incoming>| {
                    let rpc_module = rpc_module.clone();
                    let stop_handle = conn_stop.clone();
                    let svc_builder = svc_builder.clone();

                    async move {
                        if req.method() == http::Method::GET {
                            if let Some(epoch) = parse_snapshot_path(req.uri().path()) {
                                return Ok::<_, Infallible>(handle_snapshot_stream(epoch).await);
                            }
                        }

                        let mut jsonrpc_svc = svc_builder.build(rpc_module, stop_handle);
                        Ok(match jsonrpc_svc.call(req).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                tracing::error!("JSON-RPC service error: {e}");
                                http::Response::builder()
                                    .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                                    .body(HttpBody::from(format!("Internal error: {e}")))
                                    .expect("response build")
                            }
                        })
                    }
                });

                tokio::spawn(async move {
                    if let Err(e) =
                        serve_with_graceful_shutdown(sock, svc, shutdown_stop.shutdown()).await
                    {
                        tracing::error!("Connection error: {e}");
                    }
                });
            }
        });

        info!("JSON-RPC Server started at {}", addr);
        server_handle
    }
}

fn parse_snapshot_path(path: &str) -> Option<u64> {
    path.strip_prefix("/snapshots/").and_then(|rest| rest.trim_end_matches('/').parse::<u64>().ok())
}

async fn handle_snapshot_stream(epoch: u64) -> http::Response<HttpBody> {
    let snapshot_path = format!(
        "{DATA_DISK_DIR}/{SNAPSHOT_FILE_PREFIX}{epoch}/{SNAPSHOT_FILE_PREFIX}{epoch}.tar.gz",
    );

    let file = match tokio::fs::File::open(&snapshot_path).await {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            return http::Response::builder()
                .status(http::StatusCode::NOT_FOUND)
                .body(HttpBody::from(format!("No snapshot for epoch {epoch}")))
                .expect("response build");
        }
        Err(e) => {
            return http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(HttpBody::from(format!("Failed to open snapshot: {e}")))
                .expect("response build");
        }
    };

    let metadata = match file.metadata().await {
        Ok(m) => m,
        Err(e) => {
            return http::Response::builder()
                .status(http::StatusCode::INTERNAL_SERVER_ERROR)
                .body(HttpBody::from(format!("Failed to read file metadata: {e}")))
                .expect("response build");
        }
    };
    let file_size = metadata.len();

    let reader = ReaderStream::with_capacity(file, 1024 * 1024);
    let stream = reader.map(|result| result.map(Frame::data));
    let body = HttpBody::new(StreamBody::new(stream));

    http::Response::builder()
        .status(http::StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/gzip")
        .header(http::header::CONTENT_LENGTH, file_size)
        .header(
            http::header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"epoch_{epoch}.tar.gz\""),
        )
        .body(body)
        .expect("response build")
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
            return Err(string_to_rpc_error(format!("HTTP error: {}", response.status())));
        }

        let bytes = response
            .bytes()
            .await
            .map_err(|e| string_to_rpc_error(format!("Failed to read response body: {}", e)))?;

        // Create the filename
        let filename = format!("{SNAPSHOT_FILE_PREFIX}{epoch}.tar.lz4");

        // Write to file
        let mut file =
            tokio::fs::File::create(format!("{DATA_DISK_DIR}/{filename}")).await.map_err(|e| {
                string_to_rpc_error(format!("Failed to create file {}: {}", filename, e))
            })?;

        file.write_all(&bytes).await.map_err(|e| {
            string_to_rpc_error(format!("Failed to write to file {}: {}", filename, e))
        })?;

        Ok(())
    }

    /// Restores from an encrypted snapshot
    async fn restore_from_encrypted_snapshot(&self, _epoch: u64) -> RpcResult<()> {
        todo!()
    }

    /// Get an encrypted snapshot from this servers database
    async fn get_encrypted_snapshot(&self, epoch: u64) -> RpcResult<Vec<u8>> {
        tracing::warn!(
            epoch,
            "get_encrypted_snapshot is deprecated; use GET /snapshots/{{epoch}} for streaming"
        );

        let snapshot_path = format!(
            "{DATA_DISK_DIR}/{SNAPSHOT_FILE_PREFIX}{epoch}/{SNAPSHOT_FILE_PREFIX}{epoch}.tar.gz",
        );

        if !fs::exists(&snapshot_path).unwrap_or_default() {
            return Err(string_to_rpc_error(format!("No snapshot for epoch {epoch} stored")));
        }

        fs::read(snapshot_path).map_err(|e| {
            string_to_rpc_error(format!("Failed to read snapshot for epoch {}: {}", epoch, e))
        })
    }

    /// List all encrypted snapshots stored in this enclave
    async fn list_all_encrypted_snapshots(&self) -> RpcResult<Vec<u64>> {
        let dir_path = Path::new(DATA_DISK_DIR);

        let entries = fs::read_dir(dir_path).map_err(|e| {
            string_to_rpc_error(format!("Failed to read snapshots directory: {}", e))
        })?;

        let mut epochs = Vec::new();
        #[allow(clippy::useless_format)]
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
