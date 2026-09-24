//! Deterministic test-only producer using the exact Summit dependency pinned in Cargo.toml.
//! These keys and synthetic execution hashes must never be used on a real network.

use std::{collections::BTreeMap, num::NonZeroU64};

use commonware_codec::{DecodeExt, Encode};
use commonware_consensus::{
    simplex::types::{Finalization, Finalize, Proposal},
    types::{Epoch, Round, View},
};
use commonware_cryptography::{
    bls12381::{
        self,
        primitives::{
            group,
            variant::{MinPk, Variant},
        },
    },
    ed25519, Signer,
};
use commonware_parallel::Sequential;
use commonware_utils::{ordered::BiMap, TryCollect};
use serde_json::{json, Value};
use ssz::Encode as _;
use summit_types::{
    account::{ValidatorAccount, ValidatorStatus},
    checkpoint::{verify_checkpoint_chain, Checkpoint},
    consensus_state::ConsensusState,
    genesis::{Genesis, GenesisValidator},
    protocol_params::ProtocolParam,
    rpc::CheckpointRes,
    scheme::MultisigScheme,
    Block, Digest, FinalizedHeader, Header,
};

pub const SUMMIT_REVISION: &str = "b1651e7eaef0378815359ffdc4631316f78f03c4";
pub const CHECKPOINT_HEIGHT: u64 = 8;
pub const EXECUTION_HASH: [u8; 32] = [0x18; 32];
pub const RETH_STATE_ROOT: [u8; 32] = [0x28; 32];
pub const CAPTURED_EXECUTION_HASH: [u8; 32] = [0x17; 32];
pub const TERMINAL_EXECUTION_HASH: [u8; 32] = [0x19; 32];

pub fn checkpoint_state() -> (Genesis, ConsensusState) {
    let mut accounts = BTreeMap::new();
    let mut validators = Vec::new();
    for seed in 0..4 {
        let node_key = ed25519::PrivateKey::from_seed(seed).public_key();
        let consensus_key = bls12381::PrivateKey::from_seed(100 + seed).public_key();
        let account = ValidatorAccount {
            consensus_public_key: consensus_key.clone(),
            withdrawal_credentials: [seed as u8; 20].into(),
            balance: 32_000_000_000,
            status: ValidatorStatus::Active,
            joining_epoch: 0,
            last_deposit_index: 0,
        };
        validators.push(GenesisValidator {
            node_public_key: format!("0x{}", hex::encode(node_key.as_ref())),
            consensus_public_key: format!("0x{}", hex::encode(&consensus_key)),
            ip_address: format!("127.0.0.1:{}", 10_000 + seed),
            withdrawal_credentials: account.withdrawal_credentials.to_string(),
        });
        accounts.insert(node_key.as_ref().try_into().unwrap(), account);
    }
    let genesis = Genesis {
        validators,
        eth_genesis_hash: format!("0x{}", "00".repeat(32)),
        leader_timeout_ms: 1_000,
        notarization_timeout_ms: 1_000,
        nullify_timeout_ms: 1_000,
        activity_timeout_views: 10,
        skip_timeout_views: 5,
        max_message_size_bytes: 1_048_576,
        namespace: "checkpointer-new-format-fixture-v1".into(),
        validator_minimum_stake: 32_000_000_000,
        blocks_per_epoch: 10,
        allowed_timestamp_future_ms: 10_000,
        treasury_address: format!("0x{}", "00".repeat(20)),
        max_deposits_per_epoch: 3,
        max_withdrawals_per_epoch: 16,
        observers_per_validator: 0,
        max_validator_count: 256,
        minimum_validator_count: 1,
        invalid_deposit_tax: 0,
        max_pending_withdrawals_per_validator: 3,
    };
    let mut state = ConsensusState::new(
        Default::default(),
        genesis.validator_minimum_stake,
        NonZeroU64::new(genesis.blocks_per_epoch).unwrap(),
        genesis.allowed_timestamp_future_ms,
        Default::default(),
        genesis.max_deposits_per_epoch,
        genesis.max_withdrawals_per_epoch,
        genesis.observers_per_validator,
        genesis.max_validator_count,
        genesis.minimum_validator_count,
        genesis.invalid_deposit_tax,
        genesis.max_pending_withdrawals_per_validator,
    );
    state.set_validator_accounts(accounts);
    state.set_latest_height(CHECKPOINT_HEIGHT - 1);
    state.set_view(CHECKPOINT_HEIGHT - 1);
    state.set_head_digest([0x70; 32].into());
    state.set_forkchoice_head(CAPTURED_EXECUTION_HASH.into());
    state.set_forkchoice_safe_and_finalized(CAPTURED_EXECUTION_HASH.into());
    state.capture_state_root(CHECKPOINT_HEIGHT - 1);

    // Summit seals the penultimate checkpoint before capturing this block's proof
    // state. Preserve the previous capture, but advance the live state/identity.
    state.set_latest_height(CHECKPOINT_HEIGHT);
    state.set_view(CHECKPOINT_HEIGHT);
    state.set_head_digest([0x80; 32].into());
    state.set_forkchoice_head(EXECUTION_HASH.into());
    state.push_protocol_param_change(ProtocolParam::MaxValidatorCount(512));
    (genesis, state)
}

pub fn sign_header(genesis: &Genesis, header: Header) -> FinalizedHeader<MultisigScheme> {
    let validators = genesis.get_validators().expect("valid deterministic genesis");
    let participants: BiMap<_, _> = validators
        .iter()
        .map(|v| {
            let public: &<MinPk as Variant>::Public = v.consensus_public_key.as_ref();
            (v.node_public_key.clone(), *public)
        })
        .try_collect()
        .unwrap();
    let domain = summit_types::chain_domain(genesis.config_digest());
    let schemes: Vec<_> = (0..4)
        .map(|seed| {
            let encoded = bls12381::PrivateKey::from_seed(100 + seed).encode();
            let private = group::Private::decode(encoded).unwrap();
            MultisigScheme::signer(&domain, participants.clone(), private).unwrap()
        })
        .collect();
    let proposal = Proposal {
        round: Round::new(Epoch::new(header.epoch()), View::new(header.view())),
        parent: View::new(header.view() - 1),
        payload: header.computed_digest(),
    };
    let finalizes: Vec<_> = schemes
        .iter()
        .take(3)
        .map(|scheme| Finalize::sign(scheme, proposal.clone()).unwrap())
        .collect();
    let finalization = Finalization::from_finalizes(
        &schemes[0],
        commonware_utils::non_empty![@&finalizes],
        &Sequential,
    )
    .unwrap();
    // Decode/binding checks alone do not verify BLS signatures. Independently
    // verify the quorum certificate against the committee and chain domain.
    let verifier = MultisigScheme::verifier(&domain, participants);
    assert!(finalization.verify(&mut commonware_utils::sys_rng(), &verifier, &Sequential));
    FinalizedHeader::new(header, finalization, schemes.len()).unwrap()
}

pub fn terminal_artifacts(
    genesis: &Genesis,
    state: &ConsensusState,
    checkpoint_digest: Digest,
) -> (Block, FinalizedHeader<MultisigScheme>) {
    // Reuse Summit's public genesis payload constructor, but populate a distinct
    // terminal execution identity. No extra Alloy dependency is needed.
    let mut payload = Block::genesis(genesis.genesis_hash()).payload;
    let execution = &mut payload.payload_inner.payload_inner;
    execution.block_number = state.get_latest_height() + 1;
    execution.block_hash = TERMINAL_EXECUTION_HASH.into();
    execution.parent_hash = state.get_forkchoice().head_block_hash;
    execution.state_root = [0x29; 32].into();
    execution.timestamp = 1_700_000_000;
    let block = Block::compute_digest(
        state.get_head_digest(),
        state.get_latest_height() + 1,
        1_700_000_000,
        payload,
        Vec::new(),
        state.get_epoch(),
        state.get_view() + 1,
        Some(checkpoint_digest),
        genesis.genesis_hash().into(),
        Vec::new(),
        Vec::new(),
        state.get_state_root(),
    );
    let header = sign_header(genesis, block.header.clone());
    (block, header)
}

pub fn generate_fixture() -> Value {
    let (genesis, state) = checkpoint_state();
    let checkpoint = Checkpoint::new(&state);
    let (block, finalized_header) = terminal_artifacts(&genesis, &state, checkpoint.digest);
    // A single-epoch history permits upstream verification of the committee,
    // certificate, checkpoint commitment, state position and validator accounts.
    verify_checkpoint_chain(&genesis, std::slice::from_ref(&finalized_header), &checkpoint)
        .expect("fixture must pass upstream checkpoint-chain verification");
    let response = CheckpointRes {
        epoch: state.get_epoch(),
        digest: checkpoint.digest.into(),
        checkpoint: checkpoint.as_ssz_bytes(),
        last_block: block.as_ssz_bytes(),
        finalized_header: finalized_header.as_ssz_bytes(),
    };
    json!({
        "producer": {
            "summit_revision": SUMMIT_REVISION,
            "commonware_version": "2026.9.0",
            "generator": "cargo run --locked --example generate_checkpoint_fixture",
            "note": "Synthetic deterministic test keys and execution hashes; not a live snapshot"
        },
        "genesis": genesis,
        "expected": {
            "checkpoint_height": CHECKPOINT_HEIGHT,
            "execution_block_hash": format!("0x{}", hex::encode(EXECUTION_HASH)),
            "reth_state_root": format!("0x{}", hex::encode(RETH_STATE_ROOT)),
            "captured_height": CHECKPOINT_HEIGHT - 1,
            "captured_execution_block_hash": format!("0x{}", hex::encode(CAPTURED_EXECUTION_HASH)),
            "terminal_execution_block_hash": format!("0x{}", hex::encode(TERMINAL_EXECUTION_HASH)),
            "current_max_validator_count": 256,
            "prospective_max_validator_count": 512
        },
        "response": response
    })
}
