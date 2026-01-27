use jsonrpsee::{
    core::client::ClientT,
    http_client::{HttpClient, HttpClientBuilder},
};
use serde::{Deserialize, Serialize};

use crate::error::Result;

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
    pub timestamp: String,
    #[serde(rename = "parentHash")]
    pub parent_hash: String,
}
