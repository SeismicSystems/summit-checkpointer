# New-format Summit upgrade: validation and downstream handoff

## Scope

Fresh-start decoder/dependency upgrade from Checkpointer
`1da613f920f7a27e9463e0b4d9b762bf8261f34d`. No legacy decoding, checkpoint
migration, network cutover, data resets, or validator-cap API exposure.
Production identity checks, penultimate unwind scheduling, RPC/HTTP routes and
manifest version 1 are unchanged. The production Rust source change only wires
in the new test module; the decoder upgrade comes from the dependency pin.

## Reproducible dependency stack

- Summit types source:
  `git+https://github.com/SeismicSystems/summit.git?rev=b1651e7eaef0378815359ffdc4631316f78f03c4#b1651e7eaef0378815359ffdc4631316f78f03c4`
- All eleven direct Commonware constraints: `=2026.9.0`.
- Metadata confirms **19 Commonware packages, all 2026.9.0**, one Summit types
  revision, and no local-path dependencies.
- `ethereum_ssz = "0.9.0"` is unchanged. Its resolved version is **0.9.1**, also
  unchanged from the pre-upgrade lockfile; the manifest requirement is not exact.
- Lockfile updated with `cargo update -p summit-types`, not a blanket refresh.
  Commonware's changed transitive requirements account for additional dependency
  updates. Existing unrelated third-party duplicate versions remain acceptable.
- Compiler: `rustc 1.95.0 (59807616e 2026-04-14)`, Linux x86_64, LLVM 22.1.2.
- Cargo: `1.95.0 (f2d3ce0bd 2026-03-21)`.
- Formatter used: `rustfmt 1.10.0-nightly (5db7f4be8a 2026-09-01)`.
  Formatting continues to use nightly for the existing rustfmt configuration.

CI explicitly installs 1.95.0, uses the repository toolchain pin and locked
Cargo commands, checks fixture regeneration, and builds release artifacts.

## Local validation (2026-09-24)

All commands below passed from this repository (build parallelism limited with
`CARGO_BUILD_JOBS=4`):

```sh
cargo +nightly fmt --all --check
RUSTFLAGS="-D warnings" cargo check --locked --all-targets
cargo clippy --locked --all-targets -- -D warnings
cargo build --locked
cargo test --locked --all-targets
cargo run --locked --example generate_checkpoint_fixture -- --check
cargo build --locked --release
cargo tree --locked -d
cargo metadata --locked --format-version 1 > /tmp/checkpointer-upgrade-metadata.json
python3 scripts/check_old_decoder.py
git diff --check
```

- **42 unit tests passed**, 0 failed, 0 ignored (30 existing + 12 new).
  The binary and example targets also compiled and ran their empty test suites.
- Fixed fixture regenerates byte-for-byte. Its quorum certificate passes both
  independent verification in the generator and upstream checkpoint-chain
  verification; the wrong-domain negative control fails as intended.
- Production decoder rejects altered response digests, corrupted checkpoint
  data, invalid current cap, wrong epoch/height, header/certificate mismatch,
  cross-checkpoint terminal artifacts and terminal parent/height mismatches.
- Local mock RPCs exercise the production bundle writer and Reth identity checks,
  terminal chain-file equality, and manifest writer. The manifest uses the mock
  Reth header's state root, not the consensus proof root or terminal payload root.
  The exact temporary archive size/hash and complete v1 JSON schema are asserted.
- Isolated previous-decoder harness: **2 controls passed**. The unchanged old
  decoder accepts its own valid old-format bundle and rejects the fixed new
  fixture at consensus-state decode with
  `Invalid("ProtocolParam", "unknown tag")`. Its original manifest and lockfile
  remain byte-identical.

No full snapshot smoke test was run. Tests do not copy/unwind an MDBX database,
execute Reth, or access live Summit, deployment directories, or cloud storage.

## Fixture handoff to checkpoint-app

Reuse the exact bytes and provenance in
[`tests/fixtures/summit-b1651e7-checkpoint.json`](../tests/fixtures/summit-b1651e7-checkpoint.json).
See the [fixture documentation](../tests/fixtures/README.md) for generation and
old-decoder control instructions.

- Fixture file SHA-256:
  `808c267c1c59f458408a6a79eff9dcf8a12f2407e688dc56e73873cc6dad59b7`
- Checkpoint digest:
  `0x189d7a5dc92b6e6866c2fc2fb1208eb3c9d94860a0555d91e6649c268725f54d`
- Epoch 0, checkpoint execution height **8**, hash `0x18…18` (32 bytes).
- Embedded prior capture is at height 7 (`0x17…17`); terminal block is at height 9
  (`0x19…19`). Neither is the manifest's execution identity.
- Current cap 256, prospective cap 512; mock Reth state root `0x28…28`.

Publication is a separate authorized step. Until these changes are committed and
published, **there is no new Checkpointer Git revision for the app to pin**.
After publication, record the exact available revision (or verify the intended
squash-merge revision), then update checkpoint-app's Checkpointer and Summit pins,
Commonware constraints and toolchain together. Consume `response` through the
app's real `deserialize_checkpoint()` and verify the same manifest identity.
Do not commit a local-path workaround. This change does not update checkpoint-app.
