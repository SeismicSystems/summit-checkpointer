//! Regenerate the fixed wire fixture, or compare it without modifying files.

#[path = "../tests/support/checkpoint_fixture.rs"]
mod checkpoint_fixture;

use std::path::Path;

use anyhow::{bail, Result};

fn main() -> Result<()> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if !args.is_empty() && args != ["--check"] {
        bail!("usage: cargo run --locked --example generate_checkpoint_fixture -- [--check]");
    }
    let path =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/summit-b1651e7-checkpoint.json");
    let output =
        format!("{}\n", serde_json::to_string_pretty(&checkpoint_fixture::generate_fixture())?);
    if args == ["--check"] {
        if std::fs::read(&path)? != output.as_bytes() {
            bail!("{} differs from the deterministic generator", path.display());
        }
        println!("Fixture is byte-for-byte reproducible: {}", path.display());
    } else {
        std::fs::create_dir_all(path.parent().unwrap())?;
        std::fs::write(&path, output)?;
        println!("Wrote {}", path.display());
    }
    Ok(())
}
