use std::sync::Arc;

use crate::{
    config::Config,
    error::Result,
    rpc::{RethRpcClient, SummitRpcClient},
};

/// Combined RPC client wrapper
#[derive(Clone)]
pub struct RpcClient {
    pub reth: Arc<RethRpcClient>,
    pub summit: Option<Arc<SummitRpcClient>>,
}

impl RpcClient {
    /// Create a new RPC client from configuration
    pub fn new(config: &Config) -> Result<Self> {
        let reth = Arc::new(RethRpcClient::new(&config.reth.rpc_url)?);

        let summit = if config.summit.enabled {
            Some(Arc::new(SummitRpcClient::new(&config.summit.rpc_url)?))
        } else {
            tracing::info!("Summit integration disabled");
            None
        };

        Ok(Self { reth, summit })
    }
}
