use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::error::{CheckpointerError, Result};

/// Summit consensus client RPC client
pub struct SummitRpcClient {
    client: Client,
    base_url: String,
}

impl SummitRpcClient {
    /// Create a new Summit RPC client
    pub fn new(url: &str) -> Result<Self> {
        tracing::info!("Summit RPC client initialized: {}", url);
        Ok(Self { client: Client::new(), base_url: url.to_string() })
    }

    /// Get checkpoint data from Summit for a specific epoch
    pub async fn get_checkpoint(&self, epoch: u64) -> Result<CheckpointRes> {
        tracing::debug!("Fetching Summit checkpoint for epoch {}", epoch);

        // Create JSON-RPC request
        let request = json!({
            "jsonrpc": "2.0",
            "method": "getCheckpoint",
            "params": [epoch],
            "id": 1
        });

        // Send request
        let response = self.client.post(&self.base_url).json(&request).send().await?;

        if !response.status().is_success() {
            return Err(CheckpointerError::Http(response.error_for_status().unwrap_err()));
        }

        // Parse JSON-RPC response
        let rpc_response: JsonRpcResponse<CheckpointRes> = response.json().await?;

        if let Some(error) = rpc_response.error {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit RPC error: {} (code: {})",
                error.message, error.code
            )));
        }

        rpc_response.result.ok_or_else(|| {
            CheckpointerError::CheckpointExecution(
                "Summit RPC returned no result or error".to_string(),
            )
        })
    }
}

/// Summit checkpoint response structure
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct CheckpointRes {
    pub digest: [u8; 32],
    pub epoch: u64,
    pub checkpoint: Vec<u8>,
    pub last_block: Vec<u8>,
    pub finalized_header: Vec<u8>,
}

/// JSON-RPC response wrapper
#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<T>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<JsonRpcError>,
}

/// JSON-RPC error structure
#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}
