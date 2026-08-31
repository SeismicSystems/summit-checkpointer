use std::path::Path;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use crate::error::Result;

pub const SNAPSHOT_MANIFEST_FILE_NAME: &str = "manifest.json";
pub const SNAPSHOT_MANIFEST_VERSION: u32 = 1;

/// Execution-layer identity shared by Summit's finalized block and the Reth snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExecutionIdentity {
    pub block_number: u64,
    pub block_hash: [u8; 32],
    pub state_root: [u8; 32],
}

/// Public JSON manifest written alongside each Summit-backed snapshot archive.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotManifest {
    pub version: u32,
    pub epoch: u64,
    pub summit_checkpoint_digest: String,
    pub execution: ManifestExecutionIdentity,
    pub archive: ManifestArchiveIdentity,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestExecutionIdentity {
    pub block_number: u64,
    pub block_hash: String,
    pub state_root: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManifestArchiveIdentity {
    pub sha256: String,
    pub size_bytes: u64,
}

impl SnapshotManifest {
    pub fn new(
        epoch: u64,
        summit_checkpoint_digest: [u8; 32],
        execution: ExecutionIdentity,
        archive_sha256: [u8; 32],
        archive_size_bytes: u64,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            version: SNAPSHOT_MANIFEST_VERSION,
            epoch,
            summit_checkpoint_digest: hex_prefixed(&summit_checkpoint_digest),
            execution: ManifestExecutionIdentity {
                block_number: execution.block_number,
                block_hash: hex_prefixed(&execution.block_hash),
                state_root: hex_prefixed(&execution.state_root),
            },
            archive: ManifestArchiveIdentity {
                sha256: hex_prefixed(&archive_sha256),
                size_bytes: archive_size_bytes,
            },
            created_at,
        }
    }
}

/// Calculate SHA-256 over the exact archive bytes without loading the archive into memory.
pub async fn sha256_file(path: &Path) -> Result<[u8; 32]> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0_u8; 1024 * 1024];

    loop {
        let bytes_read = file.read(&mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        hasher.update(&buffer[..bytes_read]);
    }

    Ok(hasher.finalize().into())
}

pub fn hex_prefixed(value: &[u8; 32]) -> String {
    format!("0x{}", hex::encode(value))
}

#[cfg(test)]
mod tests {
    use std::time::{SystemTime, UNIX_EPOCH};

    use chrono::TimeZone;

    use super::*;

    #[test]
    fn manifest_serializes_hashes_as_prefixed_hex() {
        let created_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let manifest = SnapshotManifest::new(
            42,
            [0x11; 32],
            ExecutionIdentity { block_number: 123, block_hash: [0x22; 32], state_root: [0x33; 32] },
            [0x44; 32],
            999,
            created_at,
        );

        let json = serde_json::to_value(&manifest).unwrap();
        assert_eq!(json["version"], SNAPSHOT_MANIFEST_VERSION);
        assert_eq!(json["epoch"], 42);
        assert_eq!(json["summit_checkpoint_digest"], format!("0x{}", "11".repeat(32)));
        assert_eq!(json["execution"]["block_number"], 123);
        assert_eq!(json["execution"]["block_hash"], format!("0x{}", "22".repeat(32)));
        assert_eq!(json["execution"]["state_root"], format!("0x{}", "33".repeat(32)));
        assert_eq!(json["archive"]["sha256"], format!("0x{}", "44".repeat(32)));
        assert_eq!(json["archive"]["size_bytes"], 999);
    }

    #[tokio::test]
    async fn file_hash_is_calculated_over_exact_bytes() {
        let unique = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
        let path = std::env::temp_dir().join(format!("summit-checkpointer-hash-{unique}"));
        tokio::fs::write(&path, b"abc").await.unwrap();

        let digest = sha256_file(&path).await.unwrap();
        tokio::fs::remove_file(&path).await.unwrap();

        assert_eq!(
            hex_prefixed(&digest),
            "0xba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }
}
