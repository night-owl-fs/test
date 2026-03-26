# Step 2 Rust Downloader

`step_rust_downloader` is the Rust-first download stage for airport imagery and related step-2 repair utilities.

## Production Stage

The main pipeline step uses the crate default binary in `src/main.rs`.

Examples:

```bash
cargo run -p step_rust_downloader -- --help
```

## Rust Hotspot Repair Utility

The old `download_klga_gray_hotspot_z14_z15.py` helper has been replaced with:

- `klga_hotspot_fix`

This tool rebuilds the KLGA z14/z15 hotspot download list in Rust and can either dry-run the URLs or execute the full replacement download pass.

Dry run example:

```bash
cargo run -p step_rust_downloader --bin klga_hotspot_fix -- \
  --url-template 'https://example.com/{airport}/{z}/{x}_{y}.{ext}' \
  --root /tmp/beavery_step2_smoke \
  --dry-run
```

Real run example:

```bash
cargo run -p step_rust_downloader --bin klga_hotspot_fix -- \
  --url-template 'https://server/{airport}/{z}/{x}_{y}.{ext}' \
  --root /path/to/AIRPORT_TILES \
  --workers 24 \
  --retries 4 \
  --include-pgw
```

## Python Status

There is no longer a Python helper in this crate for the KLGA hotspot repair workflow.
