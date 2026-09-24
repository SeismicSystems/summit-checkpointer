# Summit checkpoint wire fixture

`summit-b1651e7-checkpoint.json` contains the complete JSON `getCheckpoint`
**result** under `response` (epoch, digest, checkpoint bytes, last-block bytes,
finalized-header bytes), plus producer provenance, expected identities and the
synthetic genesis configuration needed to verify the signature.

## Producer and scope

- Summit: `b1651e7eaef0378815359ffdc4631316f78f03c4`
- Commonware: `2026.9.0` (resolved by this repository's `Cargo.lock`)
- Rust: `1.95.0` (`rust-toolchain.toml`)
- Generator: `examples/generate_checkpoint_fixture.rs`, with shared test-only
  construction code in `tests/support/checkpoint_fixture.rs`

The fixture uses **test-only deterministic keys and synthetic execution hashes**.
It is a binary compatibility/identity-binding fixture, not evidence that an EVM
block was executed or that an MDBX snapshot was created. It must never be used as
a deployment genesis or a bootstrap snapshot. No historical format support or
running-network migration is introduced.

## Identities and invariants

| Item | Value |
| --- | --- |
| Epoch | 0, ten-block epoch (heights 0–9) |
| Captured proof state | Height 7; execution hash `0x17…17` |
| Outer checkpoint state | Height 8; execution hash `0x18…18` |
| Terminal block / finalized header | Height 9; execution hash `0x19…19` |
| Mock Reth header state root at height 8 | `0x28…28` |
| Terminal payload state root | `0x29…29` |
| Current validator cap | 256 |
| Queued / prospective validator cap | 512 (`MaxValidatorCount`, codec tag `0x0A`) |
| Active validators / minimum | 4 / 1 |

The prior proof capture is retained before the outer state advances to height 8,
mirroring Summit's checkpoint-before-current-capture ordering. This exercises
recursive consensus-state decoding rather than only an uncaptured synthetic
state. The outer state's head digest is the terminal block's parent; its
execution hash is also the terminal payload's parent hash. The terminal header
commits to the checkpoint digest, which equals the response digest and
SHA-256 of the checkpoint data.

Four ed25519 node keys use seeds 0–3 and four BLS keys use seeds 100–103. Three
validators sign the terminal header using `chain_domain(genesis.config_digest())`.
The generator verifies the certificate independently and calls upstream
`verify_checkpoint_chain` over the single-epoch bundle. A test also verifies that
a wrong signing domain is rejected. Checkpointer's production identity decoder
itself is **not** a full BLS/history verifier; that trust model is unchanged.

## Reproduce

From the repository root:

```sh
cargo run --locked --example generate_checkpoint_fixture          # overwrite fixture
cargo run --locked --example generate_checkpoint_fixture -- --check  # compare only
cargo test --locked --all-targets
```

Generation has no time-dependent values. `--check` compares the exact JSON bytes,
including the fixed wire bytes and provenance, and runs in CI. The wire tests
consume the checked-in fixture through Checkpointer's own `CheckpointRes` and
`decode_summit_checkpoint_identity`, not merely a runtime roundtrip.

Unit tests use in-process JSON-RPC servers bound to ephemeral loopback ports and
`tempfile` directories. They exercise the real bundle and manifest writers,
including Reth identity mismatches, terminal chain-file equality, and failure to
write a manifest before its archive exists. The archive-hashing test uses a
29-byte valid gzip of an empty tar archive and asserts a literal SHA-256 value.
No executor, external node, MDBX tool, database or cloud credentials are used.

## Previous-decoder regression control

```sh
python3 scripts/check_old_decoder.py
```

This archives Checkpointer `1da613f920f7a27e9463e0b4d9b762bf8261f34d` into a
fresh temporary source directory, preserving its original manifest and lockfile:
Summit `01a71832a140e0c774f31b27f4aad888968c1585`, Commonware `2026.7.0`.
It injects only test code and adapts the positive-control producer to the old API.
The actual old production decoder is unchanged. The two controls must execute:

1. The old decoder accepts a valid old-format bundle with a genuine certificate.
2. It rejects the checked-in new fixture specifically during consensus-state
   decoding, rather than failing to build or rejecting unrelated malformed SSZ.

The harness uses Rust 1.95.0 explicitly and `--locked`; it checks the old manifest
and lockfile are still byte-identical afterward. Its build cache defaults to
`target/old-decoder` (override `CARGO_TARGET_DIR` if desired). Temporary sources
are removed on exit. This test is not production support for old checkpoints.
