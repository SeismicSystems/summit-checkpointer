use std::time::Duration;

use reqwest::Client;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{json, Value};

use crate::error::{CheckpointerError, Result};

const CHECKPOINT_NOT_FOUND_CODE: i32 = 2000;

/// Summit consensus client RPC client.
pub struct SummitRpcClient {
    client: Client,
    base_url: String,
}

impl SummitRpcClient {
    /// Create a new Summit RPC client.
    pub fn new(url: &str, timeout: Duration) -> Result<Self> {
        if timeout.is_zero() {
            return Err(CheckpointerError::Config(
                "Summit RPC timeout must be greater than 0".to_string(),
            ));
        }

        let client = Client::builder().timeout(timeout).build()?;
        tracing::info!(url, timeout_secs = timeout.as_secs(), "Summit RPC client initialized");
        Ok(Self { client, base_url: url.to_string() })
    }

    /// Get checkpoint data from Summit for a specific epoch.
    pub async fn get_checkpoint(&self, epoch: u64) -> Result<CheckpointRes> {
        tracing::debug!("Fetching Summit checkpoint for epoch {}", epoch);
        let response: CheckpointRes = self.call("getCheckpoint", json!([epoch])).await?;
        if response.epoch != epoch {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit returned checkpoint epoch {} for requested epoch {}",
                response.epoch, epoch
            )));
        }
        Ok(response)
    }

    /// Get the latest completed Summit checkpoint epoch and digest, if one exists.
    pub async fn get_latest_checkpoint_info(&self) -> Result<Option<CheckpointInfoRes>> {
        let response = self.send("getLatestCheckpointInfo", json!([])).await?;
        response.into_optional_result("getLatestCheckpointInfo", CHECKPOINT_NOT_FOUND_CODE)
    }

    /// Get the exact historical height bounds for a Summit epoch.
    pub async fn get_epoch_bounds(&self, epoch: u64) -> Result<EpochBoundsResponse> {
        self.call("getEpochBounds", json!([epoch])).await
    }

    /// Get the SSZ-encoded finalized header for a specific epoch.
    pub async fn get_finalized_header(&self, epoch: u64) -> Result<FinalizedHeaderRes> {
        let response: FinalizedHeaderRes = self.call("getFinalizedHeader", json!([epoch])).await?;
        if response.epoch != epoch {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit returned finalized header epoch {} for requested epoch {}",
                response.epoch, epoch
            )));
        }
        if response.finalized_header.is_empty() {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit returned an empty finalized header for epoch {}",
                epoch
            )));
        }
        Ok(response)
    }

    async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T> {
        self.send(method, params).await?.into_result(method)
    }

    async fn send<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Value,
    ) -> Result<JsonRpcResponse<T>> {
        let request = json!({
            "jsonrpc": "2.0",
            "method": method,
            "params": params,
            "id": 1
        });
        let response = self.client.post(&self.base_url).json(&request).send().await?;
        if !response.status().is_success() {
            return Err(CheckpointerError::Http(response.error_for_status().unwrap_err()));
        }

        Ok(response.json().await?)
    }
}

/// Summit checkpoint response structure.
#[derive(Debug, Deserialize, Clone)]
pub struct CheckpointRes {
    pub digest: [u8; 32],
    pub epoch: u64,
    pub checkpoint: Vec<u8>,
    pub last_block: Vec<u8>,
    pub finalized_header: Vec<u8>,
}

/// Latest Summit checkpoint information.
#[derive(Debug, Deserialize, Clone)]
pub struct CheckpointInfoRes {
    pub epoch: u64,
    pub digest: [u8; 32],
}

/// Exact historical bounds for a Summit epoch.
#[derive(Debug, Deserialize, Clone, Copy)]
pub struct EpochBoundsResponse {
    pub first_height: u64,
    pub last_height: u64,
}

/// SSZ-encoded finalized header for one epoch.
#[derive(Debug, Deserialize, Clone)]
pub struct FinalizedHeaderRes {
    pub epoch: u64,
    pub finalized_header: Vec<u8>,
}

/// JSON-RPC response wrapper.
#[derive(Debug, Deserialize)]
struct JsonRpcResponse<T> {
    result: Option<T>,
    error: Option<JsonRpcError>,
}

#[derive(Debug, Deserialize)]
struct JsonRpcError {
    code: i32,
    message: String,
}

impl<T> JsonRpcResponse<T> {
    fn into_result(self, method: &str) -> Result<T> {
        if let Some(error) = self.error {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Summit RPC {method} failed: {} (code: {})",
                error.message, error.code
            )));
        }

        self.result.ok_or_else(|| {
            CheckpointerError::CheckpointExecution(format!(
                "Summit RPC {method} returned no result or error"
            ))
        })
    }

    fn into_optional_result(self, method: &str, not_found_code: i32) -> Result<Option<T>> {
        if self.error.as_ref().is_some_and(|error| error.code == not_found_code) {
            return Ok(None);
        }

        self.into_result(method).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn summit_rpc_timeout_must_be_positive() {
        assert!(SummitRpcClient::new("http://localhost:5052", Duration::ZERO).is_err());
    }

    #[test]
    fn checkpoint_not_found_is_an_empty_optional_result() {
        let response: JsonRpcResponse<CheckpointInfoRes> = JsonRpcResponse {
            result: None,
            error: Some(JsonRpcError {
                code: CHECKPOINT_NOT_FOUND_CODE,
                message: "Checkpoint not found".to_string(),
            }),
        };

        assert!(response
            .into_optional_result("getLatestCheckpointInfo", CHECKPOINT_NOT_FOUND_CODE)
            .unwrap()
            .is_none());
    }

    #[test]
    fn unexpected_rpc_errors_remain_errors() {
        let response: JsonRpcResponse<CheckpointInfoRes> = JsonRpcResponse {
            result: None,
            error: Some(JsonRpcError { code: -32603, message: "Internal error".to_string() }),
        };

        assert!(response
            .into_optional_result("getLatestCheckpointInfo", CHECKPOINT_NOT_FOUND_CODE)
            .is_err());
    }
}
