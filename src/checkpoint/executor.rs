use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::process::Command;

use crate::error::{CheckpointerError, Result};

/// Checkpoint executor for creating complete database backups
pub struct CheckpointExecutor {
    mdbx_copy_path: PathBuf,
    reth_path: PathBuf,
    compact: bool,
}

impl CheckpointExecutor {
    /// Create a new checkpoint executor
    pub fn new(mdbx_copy_path: PathBuf, reth_path: PathBuf, compact: bool) -> Self {
        Self {
            mdbx_copy_path,
            reth_path,
            compact,
        }
    }

    /// Verify that mdbx_copy and reth binaries are available and executable
    pub async fn verify_available(&self) -> Result<()> {
        // Verify mdbx_copy
        if self.mdbx_copy_path.to_str() != Some("mdbx_copy") && !self.mdbx_copy_path.exists() {
            return Err(CheckpointerError::MdbxCopyFailed(format!(
                "mdbx_copy binary not found at: {:?}",
                self.mdbx_copy_path
            )));
        }

        let output = Command::new(&self.mdbx_copy_path).arg("-V").output().await;

        match output {
            Ok(out) if out.status.success() => {
                let version = String::from_utf8_lossy(&out.stdout);
                tracing::info!("Found mdbx_copy: {}", version.trim());
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                return Err(CheckpointerError::MdbxCopyFailed(format!(
                    "mdbx_copy is not executable or invalid: {}",
                    stderr
                )));
            }
            Err(e) => {
                return Err(CheckpointerError::MdbxCopyFailed(format!(
                    "Failed to execute mdbx_copy: {}",
                    e
                )))
            }
        }

        // Verify reth
        if self.reth_path.to_str() != Some("reth") && !self.reth_path.exists() {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "reth binary not found at: {:?}",
                self.reth_path
            )));
        }

        let output = Command::new(&self.reth_path)
            .arg("--version")
            .output()
            .await;

        match output {
            Ok(out) if out.status.success() => {
                let version = String::from_utf8_lossy(&out.stdout);
                tracing::info!("Found reth: {}", version.trim());
            }
            Ok(out) => {
                let stderr = String::from_utf8_lossy(&out.stderr);
                return Err(CheckpointerError::CheckpointExecution(format!(
                    "reth is not executable or invalid: {}",
                    stderr
                )));
            }
            Err(e) => {
                return Err(CheckpointerError::CheckpointExecution(format!(
                    "Failed to execute reth: {}",
                    e
                )))
            }
        }

        Ok(())
    }

    /// Create a hot backup of the MDBX database
    pub async fn copy_mdbx_database(&self, source_db: &Path, destination: &Path) -> Result<()> {
        // Validate source exists
        if !source_db.exists() {
            return Err(CheckpointerError::MdbxCopyFailed(format!(
                "Source database does not exist: {:?}",
                source_db
            )));
        }

        // Create destination directory if needed
        if let Some(parent) = destination.parent() {
            if !parent.exists() {
                tokio::fs::create_dir_all(parent).await?;
            }
        }

        // Build command
        let mut cmd = Command::new(&self.mdbx_copy_path);
        cmd.arg("-q"); // Quiet mode

        if self.compact {
            cmd.arg("-c"); // Compact while copying
        }

        cmd.arg(source_db);
        cmd.arg(destination);

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        tracing::info!(
            "Executing mdbx_copy: source={:?}, dest={:?}, compact={}",
            source_db,
            destination,
            self.compact
        );

        let start = std::time::Instant::now();

        // Execute
        let output = cmd.output().await?;

        let duration = start.elapsed();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CheckpointerError::MdbxCopyFailed(format!(
                "mdbx_copy failed with status {}: {}",
                output.status, stderr
            )));
        }

        tracing::info!("mdbx_copy completed successfully in {:?}", duration);

        Ok(())
    }

    /// Copy static_files directory from source to destination
    pub async fn copy_static_files(&self, source_db_dir: &Path, dest_db_dir: &Path) -> Result<()> {
        let source_static = source_db_dir.join("static_files");
        let dest_static = dest_db_dir.join("static_files");

        if !source_static.exists() {
            return Err(CheckpointerError::CheckpointExecution(format!(
                "Source static_files directory does not exist: {:?}",
                source_static
            )));
        }

        tracing::info!(
            "Copying static_files from {:?} to {:?}",
            source_static,
            dest_static
        );

        let start = std::time::Instant::now();

        // Use std::fs for recursive directory copy
        copy_dir_recursive(&source_static, &dest_static)?;

        let duration = start.elapsed();

        tracing::info!("static_files copied successfully in {:?}", duration);

        Ok(())
    }

    /// Delete the lock file from static_files directory
    pub async fn delete_lock_file(&self, checkpoint_dir: &Path) -> Result<()> {
        let lock_file = checkpoint_dir.join("static_files").join("lock");

        if lock_file.exists() {
            tracing::info!("Deleting lock file: {:?}", lock_file);
            tokio::fs::remove_file(&lock_file).await?;
            tracing::debug!("Lock file deleted successfully");
        } else {
            tracing::debug!("Lock file does not exist, skipping: {:?}", lock_file);
        }

        Ok(())
    }

    /// Unwind the database to epoch_block - 2 using reth
    pub async fn unwind_database(&self, checkpoint_dir: &Path, target_block: u64) -> Result<()> {
        tracing::info!(
            "Unwinding database at {:?} to block {}",
            checkpoint_dir,
            target_block
        );

        let mut cmd = Command::new(&self.reth_path);
        cmd.arg("stage")
            .arg("unwind")
            .arg("--datadir")
            .arg(checkpoint_dir)
            .arg("to-block")
            .arg(target_block.to_string());

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let start = std::time::Instant::now();

        // Execute
        let output = cmd.output().await?;

        let duration = start.elapsed();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            let stdout = String::from_utf8_lossy(&output.stdout);
            return Err(CheckpointerError::CheckpointExecution(format!(
                "reth unwind failed with status {}: stdout={}, stderr={}",
                output.status, stdout, stderr
            )));
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        tracing::info!(
            "reth unwind completed successfully in {:?}: {}",
            duration,
            stdout.trim()
        );

        Ok(())
    }

    /// Compress checkpoint directories into a tar.gz archive and cleanup
    pub async fn compress_and_cleanup(&self, checkpoint_dir: &Path, epoch: u64) -> Result<()> {
        let archive_name = format!("epoch_{}.tar.gz", epoch);
        let archive_path = checkpoint_dir.join(&archive_name);

        tracing::info!("Compressing checkpoint to {:?}", archive_name);

        // Build tar command to compress db, static_files, and summit_checkpoint
        let mut cmd = Command::new("tar");
        cmd.arg("-czf")
            .arg(&archive_name)
            .arg("db")
            .arg("static_files");

        // Only include summit_checkpoint if it exists
        let summit_checkpoint_dir = checkpoint_dir.join("summit_checkpoint");
        if summit_checkpoint_dir.exists() {
            cmd.arg("summit_checkpoint");
        }

        // Set working directory to the checkpoint directory
        cmd.current_dir(checkpoint_dir);

        // Capture stdout/stderr
        cmd.stdout(Stdio::piped());
        cmd.stderr(Stdio::piped());

        let start = std::time::Instant::now();

        // Execute tar
        let output = cmd.output().await?;

        let duration = start.elapsed();

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(CheckpointerError::CheckpointExecution(format!(
                "tar compression failed with status {}: {}",
                output.status, stderr
            )));
        }

        tracing::info!("Compression completed in {:?}", duration);

        // Get archive size for logging
        if let Ok(metadata) = tokio::fs::metadata(&archive_path).await {
            let size_mb = metadata.len() as f64 / (1024.0 * 1024.0);
            tracing::info!("Archive size: {:.2} MB", size_mb);
        }

        // Delete the original directories
        tracing::info!("Cleaning up uncompressed directories");

        let db_dir = checkpoint_dir.join("db");
        if db_dir.exists() {
            tokio::fs::remove_dir_all(&db_dir).await?;
            tracing::debug!("Deleted db directory");
        }

        let static_files_dir = checkpoint_dir.join("static_files");
        if static_files_dir.exists() {
            tokio::fs::remove_dir_all(&static_files_dir).await?;
            tracing::debug!("Deleted static_files directory");
        }

        if summit_checkpoint_dir.exists() {
            tokio::fs::remove_dir_all(&summit_checkpoint_dir).await?;
            tracing::debug!("Deleted summit_checkpoint directory");
        }

        tracing::info!("Cleanup completed, checkpoint stored as {}", archive_name);

        Ok(())
    }
}

/// Recursively copy a directory and its contents
fn copy_dir_recursive(source: &Path, destination: &Path) -> Result<()> {
    if !destination.exists() {
        std::fs::create_dir_all(destination)?;
    }

    for entry in std::fs::read_dir(source)? {
        let entry = entry?;
        let path = entry.path();
        let dest_path = destination.join(entry.file_name());

        if path.is_dir() {
            copy_dir_recursive(&path, &dest_path)?;
        } else {
            std::fs::copy(&path, &dest_path)?;
        }
    }

    Ok(())
}
