//! Fixed-wire regression tests. All RPCs and files are disposable local fixtures;
//! these tests never invoke MDBX, Reth, or the snapshot executor.

#[path = "../../../tests/support/checkpoint_fixture.rs"]
mod fixture;

use std::{path::Path, sync::Arc};

use chrono::{TimeZone, Utc};
use jsonrpsee::{
    server::{ServerBuilder, ServerHandle},
    RpcModule,
};
use serde_json::{json, Value};
use ssz::{Decode, Encode};
use summit_types::{
    checkpoint::{verify_checkpoint_chain, Checkpoint},
    consensus_state::ConsensusState,
    genesis::Genesis,
    scheme::MultisigScheme,
    Block, FinalizedHeader,
};

use crate::{
    checkpoint::{
        manager::{decode_summit_checkpoint_identity, CheckpointManager, SummitCheckpointIdentity},
        SnapshotManifest, SNAPSHOT_MANIFEST_FILE_NAME,
    },
    config::Config,
    error::{CheckpointerError, Result},
    rpc::{summit::CheckpointRes, RpcClient},
    state::StateTracker,
};

const FIXTURE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/summit-b1651e7-checkpoint.json"
));
// Deterministic gzip of the two zero-filled end-of-archive tar blocks.
const EMPTY_TAR_GZ: &[u8] = &[
    31, 139, 8, 0, 0, 0, 0, 0, 2, 255, 99, 96, 24, 5, 163, 96, 20, 140, 84, 0, 0, 46, 175, 181,
    239, 0, 4, 0, 0,
];
const ARCHIVE_HASH: &str = "0xa1b1b81d3d1afdb8fe119b002318c12c20934713b9754a40f702adb18a2540b9";

fn fixture_json() -> Value {
    serde_json::from_str(FIXTURE).unwrap()
}

fn response() -> CheckpointRes {
    // Decode through Checkpointer's handwritten wire type, not a test-only type.
    serde_json::from_value(fixture_json()["response"].clone()).unwrap()
}

fn decode(res: &CheckpointRes) -> Result<SummitCheckpointIdentity> {
    decode_summit_checkpoint_identity(
        res.epoch,
        fixture::CHECKPOINT_HEIGHT,
        res.digest,
        &res.checkpoint,
        &res.last_block,
        &res.finalized_header,
    )
}

fn assert_execution_error<T: std::fmt::Debug>(result: Result<T>, expected: &str) {
    match result.unwrap_err() {
        CheckpointerError::CheckpointExecution(message) => {
            assert!(message.contains(expected), "expected {expected:?}, got {message:?}");
        }
        error => panic!("wrong error variant: {error:?}"),
    }
}

#[test]
fn new_wire_fixture_decodes_caps_and_penultimate_execution_identity() {
    let res = response();
    let identity = decode(&res).unwrap();
    assert_eq!(identity.checkpoint_digest, res.digest);
    assert_eq!(identity.execution_block_hash, fixture::EXECUTION_HASH);
    let checkpoint = Checkpoint::from_ssz_bytes(&res.checkpoint).unwrap();
    let state = ConsensusState::try_from(&checkpoint).unwrap();
    assert_eq!(state.get_epoch(), 0);
    assert_eq!(state.get_latest_height(), 8);
    assert_eq!(state.get_max_validator_count(), 256);
    assert_eq!(state.prospective_max_validator_count(), 512);
    assert_eq!(state.get_proof_el_block_number(), 7);
    assert_eq!(state.get_forkchoice().safe_block_hash.0, fixture::CAPTURED_EXECUTION_HASH);
    // The frozen tree is genuinely different from the live tree (which has a
    // different execution head, height and queued cap). Recursive state decoding
    // must retain it instead of rebuilding the proof snapshot from live fields.
    assert_ne!(state.get_state_root(), state.ssz_tree().root());
    assert_eq!(state.get_state_root(), state.proof_tree().root());
    assert_ne!(state.get_state_root(), fixture::RETH_STATE_ROOT);
    let block = Block::from_ssz_bytes(&res.last_block).unwrap();
    assert_eq!(block.height(), 9);
    assert_eq!(block.eth_block_hash(), fixture::TERMINAL_EXECUTION_HASH);
    assert_eq!(block.eth_parent_hash(), fixture::EXECUTION_HASH);
    assert_ne!(identity.execution_block_hash, block.eth_block_hash());
    assert_ne!(identity.execution_block_hash, fixture::CAPTURED_EXECUTION_HASH);
}

#[test]
fn checked_in_fixture_is_reproducible_and_certificate_is_valid() {
    let generated =
        format!("{}\n", serde_json::to_string_pretty(&fixture::generate_fixture()).unwrap());
    assert_eq!(FIXTURE, generated);
    assert_eq!(fixture_json()["producer"]["summit_revision"], fixture::SUMMIT_REVISION);
    let res = response();
    let genesis: Genesis = serde_json::from_value(fixture_json()["genesis"].clone()).unwrap();
    let checkpoint = Checkpoint::from_ssz_bytes(&res.checkpoint).unwrap();
    let header = FinalizedHeader::<MultisigScheme>::from_ssz_bytes(&res.finalized_header).unwrap();
    verify_checkpoint_chain(&genesis, std::slice::from_ref(&header), &checkpoint).unwrap();
    // A different signing domain must not verify, even though the artifact
    // remains well-formed and its payload/header binding still holds.
    let mut wrong_genesis = genesis;
    wrong_genesis.namespace.push_str("-wrong-domain");
    let error = verify_checkpoint_chain(&wrong_genesis, &[header], &checkpoint).unwrap_err();
    assert!(matches!(
        error,
        summit_types::checkpoint::CheckpointVerificationError::SignatureVerificationFailed {
            epoch: 0
        }
    ));
}

#[test]
fn rejects_altered_response_digest() {
    let mut res = response();
    res.digest[0] ^= 1;
    assert_execution_error(decode(&res), "Summit checkpoint digest mismatch");
}

#[test]
fn rejects_checkpoint_data_corruption_at_digest_binding() {
    let mut res = response();
    // SSZ's fixed section is offset (4) + digest (32); mutate state data while
    // preserving the SSZ envelope and advertised digest.
    res.checkpoint[36] ^= 1;
    assert_execution_error(decode(&res), "checkpoint digest does not match sha256(data)");
}

#[test]
fn rejects_invalid_current_cap_even_with_recomputed_checkpoint_digest() {
    let mut res = response();
    let (_, mut state) = fixture::checkpoint_state();
    state.set_max_validator_count(0);
    let checkpoint = Checkpoint::new(&state);
    res.checkpoint = checkpoint.as_ssz_bytes();
    res.digest = checkpoint.digest.into();
    assert_execution_error(decode(&res), "max validator count out of bounds");
}

#[test]
fn rejects_header_not_bound_to_its_certificate_payload() {
    let mut res = response();
    let honest = FinalizedHeader::<MultisigScheme>::from_ssz_bytes(&res.finalized_header).unwrap();
    let (genesis, mut state) = fixture::checkpoint_state();
    state.set_head_digest([0x99; 32].into());
    let (other_block, _) = fixture::terminal_artifacts(&genesis, &state, res.digest.into());
    // The unchecked constructor exists upstream, but the wire decoder must not
    // accept a different header paired with the original certificate.
    let mismatched = FinalizedHeader::<MultisigScheme>::new_unchecked(
        other_block.header,
        honest.finalization().clone(),
        honest.participant_count(),
    );
    res.finalized_header = mismatched.as_ssz_bytes();
    assert_execution_error(decode(&res), "finalization payload does not match the header digest");
}

#[test]
fn rejects_wrong_requested_epoch_and_height() {
    let res = response();
    for (epoch, height) in [(1, 8), (0, 9)] {
        assert_execution_error(
            decode_summit_checkpoint_identity(
                epoch,
                height,
                res.digest,
                &res.checkpoint,
                &res.last_block,
                &res.finalized_header,
            ),
            "Summit checkpoint state position mismatch",
        );
    }
}

#[test]
fn rejects_individually_valid_terminal_artifacts_from_another_checkpoint() {
    let mut res = response();
    let (genesis, mut state) = fixture::checkpoint_state();
    state.set_forkchoice_head([0x99; 32].into());
    let other_checkpoint = Checkpoint::new(&state);
    let (other_block, other_header) =
        fixture::terminal_artifacts(&genesis, &state, other_checkpoint.digest);
    // Prove the replacement bundle works independently before mixing artifacts.
    decode_summit_checkpoint_identity(
        0,
        8,
        other_checkpoint.digest.into(),
        &other_checkpoint.as_ssz_bytes(),
        &other_block.as_ssz_bytes(),
        &other_header.as_ssz_bytes(),
    )
    .unwrap();
    res.finalized_header = other_header.as_ssz_bytes();
    assert_execution_error(decode(&res), "Summit finalized-header checkpoint hash mismatch");
    let mut res = response();
    res.last_block = other_block.as_ssz_bytes();
    assert_execution_error(decode(&res), "Summit last block/finalized header digest mismatch");
}

#[test]
fn rejects_validly_signed_terminal_pair_with_wrong_parent_or_height() {
    let original = response();
    let (genesis, state) = fixture::checkpoint_state();
    for wrong_parent in [true, false] {
        let mut state = state.clone();
        if wrong_parent {
            state.set_head_digest([0x99; 32].into());
        } else {
            state.set_latest_height(9);
        }
        let (block, header) = fixture::terminal_artifacts(&genesis, &state, original.digest.into());
        let mut res = original.clone();
        res.last_block = block.as_ssz_bytes();
        res.finalized_header = header.as_ssz_bytes();
        assert_execution_error(
            decode(&res),
            if wrong_parent {
                "Summit checkpoint state/last block parent mismatch"
            } else {
                "Summit finalized header position mismatch"
            },
        );
    }
}

struct LocalRpc {
    url: String,
    handle: ServerHandle,
}

impl Drop for LocalRpc {
    fn drop(&mut self) {
        let _ = self.handle.stop();
    }
}

async fn local_rpc(reth_number: u64, reth_hash: [u8; 32], mismatch_chain: bool) -> LocalRpc {
    let server = ServerBuilder::default().build("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", server.local_addr().unwrap());
    let mut module = RpcModule::new(fixture_json());
    module
        .register_method("getCheckpoint", |params, ctx, _| {
            assert_eq!(params.parse::<(u64,)>().unwrap(), (0,));
            ctx["response"].clone()
        })
        .unwrap();
    module
        .register_method("eth_getBlockByNumber", move |params, _, _| {
            assert_eq!(params.parse::<(String, bool)>().unwrap(), ("0x8".into(), false));
            json!({
                "number": format!("0x{reth_number:x}"),
                "hash": format!("0x{}", hex::encode(reth_hash)),
                "stateRoot": format!("0x{}", hex::encode(fixture::RETH_STATE_ROOT)),
                "timestamp": "0x1",
                "parentHash": format!("0x{}", hex::encode(fixture::CAPTURED_EXECUTION_HASH))
            })
        })
        .unwrap();
    module
        .register_method("getFinalizedHeader", move |params, ctx, _| {
            assert_eq!(params.parse::<(u64,)>().unwrap(), (0,));
            let mut bytes: Vec<u8> =
                serde_json::from_value(ctx["response"]["finalized_header"].clone()).unwrap();
            if mismatch_chain {
                // Independently valid, freshly signed terminal header, not junk SSZ.
                let (genesis, mut state) = fixture::checkpoint_state();
                state.set_head_digest([0x99; 32].into());
                let checkpoint = Checkpoint::new(&state);
                bytes = fixture::terminal_artifacts(&genesis, &state, checkpoint.digest)
                    .1
                    .as_ssz_bytes();
            }
            json!({"epoch": 0, "finalized_header": bytes})
        })
        .unwrap();
    let handle = server.start(module);
    LocalRpc { url, handle }
}

async fn manager(root: &Path, url: &str) -> CheckpointManager {
    // Never load the user's config.toml or environment. Every filesystem path
    // is inside a TempDir, and no executor method is called by these tests.
    let config: Config = serde_json::from_value(json!({
        "reth": {"rpc_url": url, "db_path": root.join("unused-db")},
        "checkpoint": {
            "epoch_blocks": 10, "checkpoint_delay_blocks": 3,
            "output_dir": root.join("output"), "compact": false,
            "mdbx_copy_path": root.join("never-execute-mdbx-copy"),
            "reth_path": root.join("never-execute-reth"), "max_snapshots": null
        },
        "summit": {"enabled": true, "rpc_url": url, "rpc_timeout_secs": 5},
        "monitor": {"poll_interval_secs": 30, "retry_interval_secs": 60},
        "state": {"state_file": root.join("state.cbor")},
        "logging": {"level": "info", "format": "pretty"}
    }))
    .unwrap();
    let state = Arc::new(StateTracker::load(&config.state.state_file).await.unwrap());
    let rpc = RpcClient::new(&config).unwrap();
    CheckpointManager::new(&config, state, rpc)
}

#[tokio::test]
async fn verified_fixture_writes_bundle_and_manifest_over_exact_archive_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let rpc = local_rpc(8, fixture::EXECUTION_HASH, false).await;
    let manager = manager(dir.path(), &rpc.url).await;
    let output = dir.path().join("snapshot");
    let identity = manager
        .write_summit_checkpoint(
            manager.rpc_client.summit.as_ref().unwrap(),
            &output,
            0,
            8,
            Some(response().digest),
        )
        .await
        .unwrap();
    assert_eq!(identity.execution.block_number, 8);
    assert_eq!(identity.execution.block_hash, fixture::EXECUTION_HASH);
    assert_eq!(identity.execution.state_root, fixture::RETH_STATE_ROOT);
    let bundle = output.join("summit_checkpoint");
    assert_eq!(tokio::fs::read(bundle.join("checkpoint")).await.unwrap(), response().checkpoint);
    assert_eq!(tokio::fs::read(bundle.join("last_block")).await.unwrap(), response().last_block);
    assert_eq!(
        tokio::fs::read(bundle.join("finalized_header")).await.unwrap(),
        response().finalized_header
    );
    assert_eq!(
        tokio::fs::read(bundle.join("finalized_headers/0")).await.unwrap(),
        response().finalized_header
    );
    let created_at = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
    // The real manifest writer must not publish a sidecar before the archive.
    assert!(manager.write_snapshot_manifest(&output, 0, created_at, identity).await.is_err());
    assert!(!output.join(SNAPSHOT_MANIFEST_FILE_NAME).exists());
    tokio::fs::write(output.join("epoch_0.tar.gz"), EMPTY_TAR_GZ).await.unwrap();
    manager.write_snapshot_manifest(&output, 0, created_at, identity).await.unwrap();
    let bytes = tokio::fs::read(output.join(SNAPSHOT_MANIFEST_FILE_NAME)).await.unwrap();
    let manifest: SnapshotManifest = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        serde_json::to_value(manifest).unwrap(),
        json!({
            "version": 1,
            "epoch": 0,
            "summit_checkpoint_digest": format!("0x{}", hex::encode(response().digest)),
            "execution": {
                "block_number": 8,
                "block_hash": format!("0x{}", hex::encode(fixture::EXECUTION_HASH)),
                "state_root": format!("0x{}", hex::encode(fixture::RETH_STATE_ROOT))
            },
            "archive": {"sha256": ARCHIVE_HASH, "size_bytes": 29},
            "created_at": "2023-11-14T22:13:20Z"
        })
    );
}

#[tokio::test]
async fn fixture_rejects_reth_number_and_hash_mismatches_before_bundle_publication() {
    for (number, hash, expected) in [
        (9, fixture::EXECUTION_HASH, "Reth returned block 9"),
        (8, fixture::TERMINAL_EXECUTION_HASH, "Summit checkpoint/Reth block hash mismatch"),
        (8, fixture::CAPTURED_EXECUTION_HASH, "Summit checkpoint/Reth block hash mismatch"),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let rpc = local_rpc(number, hash, false).await;
        let manager = manager(dir.path(), &rpc.url).await;
        let output = dir.path().join("snapshot");
        let result = manager
            .write_summit_checkpoint(
                manager.rpc_client.summit.as_ref().unwrap(),
                &output,
                0,
                8,
                Some(response().digest),
            )
            .await;
        assert_execution_error(result, expected);
        assert!(!output.exists());
    }
}

#[tokio::test]
async fn fixture_rejects_mismatched_terminal_chain_file() {
    let dir = tempfile::tempdir().unwrap();
    let rpc = local_rpc(8, fixture::EXECUTION_HASH, true).await;
    let manager = manager(dir.path(), &rpc.url).await;
    let output = dir.path().join("snapshot");
    let result = manager
        .write_summit_checkpoint(
            manager.rpc_client.summit.as_ref().unwrap(),
            &output,
            0,
            8,
            Some(response().digest),
        )
        .await;
    assert_execution_error(
        result,
        "Finalized header chain terminal for epoch 0 does not match getCheckpoint",
    );
    assert!(!output.join("summit_checkpoint/checkpoint").exists());
    assert!(!output.join(SNAPSHOT_MANIFEST_FILE_NAME).exists());
}
