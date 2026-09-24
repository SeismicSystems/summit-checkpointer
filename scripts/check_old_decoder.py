#!/usr/bin/env python3
"""Run the pre-upgrade production decoder in a disposable source tree.

No pins, working files, deployment data, or Git refs are changed. Cargo's build
cache defaults to target/old-decoder (override with CARGO_TARGET_DIR). Requires
Python 3.12+, git, Cargo and Rust 1.95.0; it may fetch the old pinned dependencies.
"""

import io
import os
from pathlib import Path
import subprocess
import tarfile
import tempfile


BASELINE = "1da613f920f7a27e9463e0b4d9b762bf8261f34d"
OLD_SUMMIT = "01a71832a140e0c774f31b27f4aad888968c1585"
ROOT = Path(__file__).resolve().parents[1]


def replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise RuntimeError(f"fixture producer changed; review old-API adaptation: {old!r}")
    return source.replace(old, new, 1)


def main() -> None:
    archive = subprocess.check_output(["git", "archive", BASELINE], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix="checkpointer-old-decoder-") as directory:
        work = Path(directory)
        with tarfile.open(fileobj=io.BytesIO(archive)) as files:
            files.extractall(work, filter="data")

        # Adapt only the *positive-control producer* to the old API. The old
        # manager decoder and its Cargo manifest/lockfile remain unmodified.
        producer = (ROOT / "tests/support/checkpoint_fixture.rs").read_text()
        replacements = [
            ("b1651e7eaef0378815359ffdc4631316f78f03c4", OLD_SUMMIT),
            ('"commonware_version": "2026.9.0"', '"commonware_version": "2026.7.0"'),
            ("    protocol_params::ProtocolParam,\n", ""),
            ("        max_validator_count: 256,\n", ""),
            ("        genesis.max_validator_count,\n", ""),
            ("    state.push_protocol_param_change(ProtocolParam::MaxValidatorCount(512));\n", ""),
            ("commonware_utils::non_empty![@&finalizes]", "&finalizes"),
            ('            "current_max_validator_count": 256,\n', ""),
            ('            "prospective_max_validator_count": 512\n', ""),
        ]
        for old, new in replacements:
            producer = replace_once(producer, old, new)
        (work / "src/checkpoint/old_fixture.rs").write_text(producer)
        fixture = ROOT / "tests/fixtures/summit-b1651e7-checkpoint.json"
        (work / "new-checkpoint.json").write_bytes(fixture.read_bytes())
        manager = work / "src/checkpoint/manager.rs"
        with manager.open("a") as output:
            output.write(r'''

#[cfg(test)]
#[path = "old_fixture.rs"]
mod old_fixture;

#[cfg(test)]
mod wire_regression_control {
    use super::{decode_summit_checkpoint_identity, old_fixture};
    use crate::{error::CheckpointerError, rpc::summit::CheckpointRes};

    #[test]
    fn previous_decoder_accepts_its_own_valid_format() {
        let fixture = old_fixture::generate_fixture();
        let res: CheckpointRes = serde_json::from_value(fixture["response"].clone()).unwrap();
        let identity = decode_summit_checkpoint_identity(
            res.epoch, 8, res.digest, &res.checkpoint, &res.last_block, &res.finalized_header,
        ).expect("old-format positive control must pass the actual previous decoder");
        assert_eq!(identity.execution_block_hash, [0x18; 32]);
        assert_eq!(identity.checkpoint_digest, res.digest);
    }

    #[test]
    fn previous_decoder_rejects_fixed_new_format_at_state_decode() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"), "/new-checkpoint.json"
        ))).unwrap();
        let res: CheckpointRes = serde_json::from_value(fixture["response"].clone()).unwrap();
        let error = decode_summit_checkpoint_identity(
            res.epoch, 8, res.digest, &res.checkpoint, &res.last_block, &res.finalized_header,
        ).expect_err("the new wire format must not pass the previous decoder");
        match error {
            CheckpointerError::CheckpointExecution(message) => {
                assert!(message.contains("Failed to decode Summit checkpoint state"), "{message}");
                println!("Expected old-decoder incompatibility: {message}");
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }
}
''')
        env = os.environ.copy()
        env.setdefault("CARGO_TARGET_DIR", str(ROOT / "target/old-decoder"))
        env.setdefault("CARGO_BUILD_JOBS", "4")
        print(f"Testing Checkpointer {BASELINE}, Summit {OLD_SUMMIT} in {work}", flush=True)
        subprocess.run(
            ["cargo", "+1.95.0", "test", "--locked", "--lib", "wire_regression_control", "--", "--nocapture"],
            cwd=work, env=env, check=True,
        )
        # This must never resolve using the upgraded working tree's pins.
        for name in ("Cargo.toml", "Cargo.lock"):
            expected = subprocess.check_output(["git", "show", f"{BASELINE}:{name}"], cwd=ROOT)
            if (work / name).read_bytes() != expected:
                raise RuntimeError(f"old harness unexpectedly changed {name}")
        print("Old-format positive control and new-format rejection both passed.", flush=True)


if __name__ == "__main__":
    main()
