# Step 7 Rust Adios Trim

## Default Path

Use `rust_trim_job` by default for step 7.

```bash
cargo run -p rust_trim_job -- --help
```

## What It Does

- Copies the input tile DB to a new output DB.
- Re-encodes tile blobs to JPEG.
- Supports zoom filtering.
- Supports alpha-aware skipping and background compositing.
- Preserves the rest of the database structure because the output starts as a copy of the input DB.

## Fallback

`adios_mfer_trim_job.py` remains in the crate as the legacy fallback.

The main pipeline now runs step 7 as Rust-first and automatically retries this Python script only if the Rust trim command fails.

## Detailed Notes

See [trim_rust.md](/Users/raverynfs/Projects/BEAVERY/crates/7_step_rust_adios_trim/trim_rust.md).
