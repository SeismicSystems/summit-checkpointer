use clap::Parser;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::error::{CheckpointerError, Result};

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub reth: RethConfig,
    pub checkpoint: CheckpointConfig,
    pub summit: SummitConfig,
    pub monitor: MonitorConfig,
    pub state: StateConfig,
    pub logging: LoggingConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RethConfig {
    pub rpc_url: String,
    pub db_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointConfig {
    pub epoch_blocks: u64,
    pub checkpoint_delay_blocks: u64,
    pub output_dir: PathBuf,
    pub compact: bool,
    pub mdbx_copy_path: PathBuf,
    pub reth_path: PathBuf,
    pub max_snapshots: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SummitConfig {
    pub enabled: bool,
    pub rpc_url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MonitorConfig {
    pub poll_interval_secs: u64,
    pub retry_interval_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    pub state_file: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    pub level: String,
    pub format: String,
}

/// CLI argument parser
#[derive(Parser, Debug)]
#[command(name = "summit-checkpointer")]
#[command(version, about = "Monitor reth and create checkpoints at epoch boundaries")]
pub struct Cli {
    /// Path to configuration file
    #[arg(short, long, default_value = "config.toml")]
    pub config: PathBuf,

    /// Override reth RPC URL
    #[arg(long)]
    pub reth_rpc_url: Option<String>,

    /// Override reth database path
    #[arg(long)]
    pub reth_db_path: Option<PathBuf>,

    /// Override epoch block count
    #[arg(long)]
    pub epoch_blocks: Option<u64>,

    /// Override checkpoint delay blocks
    #[arg(long)]
    pub checkpoint_delay_blocks: Option<u64>,

    /// Override checkpoint output directory
    #[arg(long)]
    pub output_dir: Option<PathBuf>,

    /// Maximum number of snapshots to retain (unlimited if not set)
    #[arg(long)]
    pub max_snapshots: Option<u64>,

    /// Override mdbx_copy binary path
    #[arg(long)]
    pub mdbx_copy_path: Option<PathBuf>,

    /// Override reth binary path
    #[arg(long)]
    pub reth_path: Option<PathBuf>,

    /// Override log level (trace, debug, info, warn, error)
    #[arg(long)]
    pub log_level: Option<String>,

    /// Override log format (json, pretty)
    #[arg(long)]
    pub log_format: Option<String>,

    #[arg(short = 'p', long, default_value = "42069")]
    pub port: u16,
}

impl Config {
    /// Load configuration from file, environment variables, and CLI arguments
    pub fn load(cli: &Cli) -> Result<Self> {
        // Start with default configuration
        let mut builder = config::Config::builder()
            .set_default("reth.rpc_url", "http://localhost:8545")?
            .set_default("reth.db_path", "/var/lib/reth/db")?
            .set_default("checkpoint.epoch_blocks", 1000)?
            .set_default("checkpoint.checkpoint_delay_blocks", 3)?
            .set_default("checkpoint.output_dir", "/var/lib/reth/checkpoints")?
            .set_default("checkpoint.compact", true)?
            .set_default("checkpoint.mdbx_copy_path", "mdbx_copy")?
            .set_default("checkpoint.reth_path", "reth")?
            .set_default("checkpoint.max_snapshots", None::<u64>)?
            .set_default("summit.enabled", false)?
            .set_default("summit.rpc_url", "http://localhost:5052")?
            .set_default("monitor.poll_interval_secs", 12)?
            .set_default("monitor.retry_interval_secs", 60)?
            .set_default("state.state_file", "./checkpointer_state.cbor")?
            .set_default("logging.level", "info")?
            .set_default("logging.format", "pretty")?;

        // Load from config file if it exists
        if cli.config.exists() {
            builder = builder.add_source(config::File::from(cli.config.clone()));
        }

        // Override with environment variables (e.g., CHECKPOINTER_RETH_RPC_URL)
        builder = builder.add_source(
            config::Environment::with_prefix("CHECKPOINTER").separator("_").try_parsing(true),
        );

        // Build the config
        let mut config: Config = builder.build()?.try_deserialize()?;

        // Apply CLI overrides
        if let Some(rpc_url) = &cli.reth_rpc_url {
            config.reth.rpc_url = rpc_url.clone();
        }
        if let Some(db_path) = &cli.reth_db_path {
            config.reth.db_path = db_path.clone();
        }
        if let Some(epoch_blocks) = cli.epoch_blocks {
            config.checkpoint.epoch_blocks = epoch_blocks;
        }
        if let Some(checkpoint_delay_blocks) = cli.checkpoint_delay_blocks {
            config.checkpoint.checkpoint_delay_blocks = checkpoint_delay_blocks;
        }
        if let Some(output_dir) = &cli.output_dir {
            config.checkpoint.output_dir = output_dir.clone();
        }
        if let Some(max_snapshots) = cli.max_snapshots {
            config.checkpoint.max_snapshots = Some(max_snapshots);
        }
        if let Some(mdbx_copy_path) = &cli.mdbx_copy_path {
            config.checkpoint.mdbx_copy_path = mdbx_copy_path.clone();
        }
        if let Some(reth_path) = &cli.reth_path {
            config.checkpoint.reth_path = reth_path.clone();
        }
        if let Some(log_level) = &cli.log_level {
            config.logging.level = log_level.clone();
        }
        if let Some(log_format) = &cli.log_format {
            config.logging.format = log_format.clone();
        }

        // Expand tilde in paths
        config.reth.db_path = expand_path(&config.reth.db_path)?;
        config.checkpoint.output_dir = expand_path(&config.checkpoint.output_dir)?;
        config.state.state_file = expand_path(&config.state.state_file)?;

        // Validate configuration
        config.validate()?;

        Ok(config)
    }

    /// Validate configuration values
    fn validate(&self) -> Result<()> {
        // Validate epoch_blocks
        if self.checkpoint.epoch_blocks == 0 {
            return Err(CheckpointerError::Config(
                "checkpoint.epoch_blocks must be greater than 0".to_string(),
            ));
        }

        // Validate poll_interval_secs
        if self.monitor.poll_interval_secs == 0 {
            return Err(CheckpointerError::Config(
                "monitor.poll_interval_secs must be greater than 0".to_string(),
            ));
        }

        // Validate retry_interval_secs
        if self.monitor.retry_interval_secs == 0 {
            return Err(CheckpointerError::Config(
                "monitor.retry_interval_secs must be greater than 0".to_string(),
            ));
        }

        // Validate log level
        let valid_levels = ["trace", "debug", "info", "warn", "error"];
        if !valid_levels.contains(&self.logging.level.as_str()) {
            return Err(CheckpointerError::Config(format!(
                "logging.level must be one of: {}",
                valid_levels.join(", ")
            )));
        }

        // Validate log format
        let valid_formats = ["json", "pretty"];
        if !valid_formats.contains(&self.logging.format.as_str()) {
            return Err(CheckpointerError::Config(format!(
                "logging.format must be one of: {}",
                valid_formats.join(", ")
            )));
        }

        Ok(())
    }
}

/// Expand tilde and environment variables in paths
#[allow(clippy::ptr_arg)]
fn expand_path(path: &PathBuf) -> Result<PathBuf> {
    let path_str = path
        .to_str()
        .ok_or_else(|| CheckpointerError::InvalidPath("Invalid UTF-8 in path".to_string()))?;

    let expanded = shellexpand::full(path_str)
        .map_err(|e| CheckpointerError::InvalidPath(format!("Failed to expand path: {}", e)))?;

    Ok(PathBuf::from(expanded.as_ref()))
}
