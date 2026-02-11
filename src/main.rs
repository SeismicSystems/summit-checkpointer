use clap::Parser;
use std::{net::SocketAddr, sync::Arc};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

use summit_checkpointer::{
    BlockMonitor, CheckpointManager, Cli, Config, Result, RpcClient, RpcServer, StateTracker,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Parse CLI arguments
    let cli = Cli::parse();

    // Load configuration
    let config = Config::load(&cli)?;

    // Initialize logging
    init_logging(&config)?;

    tracing::info!("Summit-Checkpointer starting up");
    tracing::info!(
        "Configuration: reth_url={}, epoch_blocks={}, output_dir={:?}",
        config.reth.rpc_url,
        config.checkpoint.epoch_blocks,
        config.checkpoint.output_dir
    );

    // Load state tracker
    let state_tracker = Arc::new(StateTracker::load(&config.state.state_file).await?);

    // Create RPC clients
    let rpc_client = RpcClient::new(&config)?;

    // Create checkpoint manager
    let checkpoint_manager =
        Arc::new(CheckpointManager::new(&config, state_tracker.clone(), rpc_client.clone()));

    // Verify checkpoint tools are available
    tracing::info!("Verifying checkpoint tools (mdbx_copy, reth)...");
    checkpoint_manager.verify_checkpoint_tools().await?;

    // Setup graceful shutdown signal handling
    let shutdown_token = CancellationToken::new();
    let shutdown_signal = shutdown_token.clone();

    tokio::spawn(async move {
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                tracing::info!("Received shutdown signal (Ctrl-C)");
                shutdown_signal.cancel();
            }
            Err(err) => {
                tracing::error!("Unable to listen for shutdown signal: {}", err);
            }
        }
    });

    // Create and run block monitor
    let mut monitor = BlockMonitor::new(
        rpc_client,
        checkpoint_manager,
        config.monitor.clone(),
        config.checkpoint.epoch_blocks,
        config.checkpoint.checkpoint_delay_blocks,
    );

    tracing::info!("Block monitor initialized, starting main loop");
    let addr: SocketAddr = format!("0.0.0.0:{}", cli.port).parse().unwrap();

    let rpc_handle = tokio::spawn(RpcServer::new().start_server(addr));
    // Run until cancelled
    monitor.run_until_cancelled(shutdown_token).await?;

    rpc_handle.abort();

    // Cleanup: save state
    tracing::info!("Saving final state...");
    state_tracker.save().await?;

    tracing::info!("Summit-Checkpointer shutdown complete");

    Ok(())
}

/// Initialize logging with tracing subscriber
fn init_logging(config: &Config) -> Result<()> {
    let env_filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(&config.logging.level));

    let subscriber = tracing_subscriber::registry().with(env_filter);

    match config.logging.format.as_str() {
        "json" => {
            subscriber.with(fmt::layer().json()).init();
        }
        "pretty" => {
            subscriber.with(fmt::layer().pretty()).init();
        }
        _ => {
            subscriber.with(fmt::layer()).init();
        }
    }

    Ok(())
}
