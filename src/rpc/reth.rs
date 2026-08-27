use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
};
use serde::{Deserialize, Serialize};

use crate::error::{CheckpointerError, Result};

/// Reth RPC client for interacting with execution layer
pub struct RethRpcClient {
    client: HttpClient,
}

impl RethRpcClient {
    /// Create a new Reth RPC client
    pub fn new(url: &str) -> Result<Self> {
        let client = HttpClientBuilder::default().build(url)?;

        tracing::info!("Reth RPC client initialized: {}", url);
        Ok(Self { client })
    }

    /// Get current block number from reth
    pub async fn get_block_number(&self) -> Result<u64> {
        let response: String = self
            .client
            .request("eth_blockNumber", jsonrpsee::core::params::ArrayParams::new())
            .await?;

        // Parse hex string to u64 (format: "0x...")
        let block_num = u64::from_str_radix(response.trim_start_matches("0x"), 16)?;

        tracing::trace!("Current block number: {}", block_num);
        Ok(block_num)
    }

    /// Get block by number (optional - for additional metadata)
    pub async fn get_block(&self, block_num: u64) -> Result<BlockInfo> {
        let block_param = format!("0x{:x}", block_num);
        let mut params = jsonrpsee::core::params::ArrayParams::new();
        params.insert(block_param)?;
        params.insert(false)?; // Don't include full transactions

        let response: BlockInfo = self.client.request("eth_getBlockByNumber", params).await?;

        Ok(response)
    }
}

/// Block information structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlockInfo {
    pub number: String,
    pub hash: String,
    #[serde(rename = "stateRoot")]
    pub state_root: String,
    pub timestamp: String,
    #[serde(rename = "parentHash")]
    pub parent_hash: String,
}

impl BlockInfo {
    pub fn block_number(&self) -> Result<u64> {
        u64::from_str_radix(self.number.trim_start_matches("0x"), 16).map_err(Into::into)
    }

    pub fn block_hash(&self) -> Result<[u8; 32]> {
        parse_hash("hash", &self.hash)
    }

    pub fn state_root(&self) -> Result<[u8; 32]> {
        parse_hash("stateRoot", &self.state_root)
    }
}

fn parse_hash(field: &str, value: &str) -> Result<[u8; 32]> {
    let bytes = hex::decode(value.trim_start_matches("0x")).map_err(|error| {
        CheckpointerError::Parse(format!("invalid Reth {field} hex value {value:?}: {error}"))
    })?;

    bytes.try_into().map_err(|bytes: Vec<u8>| {
        CheckpointerError::Parse(format!(
            "invalid Reth {field} length: expected 32 bytes, received {}",
            bytes.len()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn block_info() -> BlockInfo {
        BlockInfo {
            number: "0x2a".to_string(),
            hash: format!("0x{}", "11".repeat(32)),
            state_root: format!("0x{}", "22".repeat(32)),
            timestamp: "0x1".to_string(),
            parent_hash: format!("0x{}", "00".repeat(32)),
        }
    }

    #[test]
    fn parses_execution_identity_fields() {
        let block = block_info();

        assert_eq!(block.block_number().unwrap(), 42);
        assert_eq!(block.block_hash().unwrap(), [0x11; 32]);
        assert_eq!(block.state_root().unwrap(), [0x22; 32]);
    }

    #[test]
    fn rejects_invalid_hash_lengths() {
        let mut block = block_info();
        block.hash = "0x11".to_string();

        assert!(block.block_hash().is_err());
    }
}
