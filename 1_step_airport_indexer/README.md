# Step 1 Airport Indexer

`step_airport_indexer` is the Rust-first entry stage for airport selection, cone generation, and step-1 support tooling.

## Production Stage

The main pipeline step uses the crate default binary in `src/main.rs`.

Examples:

```bash
cargo run -p step_airport_indexer -- --help
```

## Rust Toolkit Replacement For Legacy Python Helpers

The legacy step 1 helper scripts were consolidated into one Rust maintenance binary:

- `airport_index_toolkit`

It covers the jobs that were previously spread across:

- `build_all_airports_from_minimal_specs.py`
- `build_cones_from_tile_index.py`
- `spec_generator.py`
- `spec_generator2.py`
- `spec_generator3.py`
- `spec_manual.py`

## Common Commands

Generate minimal specs from the checked-in manual seed file:

```bash
cargo run -p step_airport_indexer --bin airport_index_toolkit -- \
  generate-specs \
  --source manual \
  --out-dir /tmp/beavery_step1_specs
```

Generate specs from the tile index file:

```bash
cargo run -p step_airport_indexer --bin airport_index_toolkit -- \
  generate-specs \
  --source index \
  --index-file crates/1_step_airport_indexer/TILE_INDEX_MASTER_KEY_fixed_kmwl.txt \
  --out-dir /tmp/beavery_step1_specs
```

Build GeoTIFF groups from generated spec JSON files:

```bash
cargo run -p step_airport_indexer --bin airport_index_toolkit -- \
  build-geotiffs \
  --specs-dir /tmp/beavery_step1_specs \
  --out-dir /tmp/beavery_step1_geotiffs
```

Run the full index-to-GeoTIFF preparation flow:

```bash
cargo run -p step_airport_indexer --bin airport_index_toolkit -- \
  build-from-index \
  --index-file crates/1_step_airport_indexer/TILE_INDEX_MASTER_KEY_fixed_kmwl.txt \
  --out-dir /tmp/beavery_step1_specs \
  --geotiff-out-dir /tmp/beavery_step1_geotiffs
```

## Compatibility Wrapper

`legacy_airport_tools.py` still exists only so old operator habits and notes do not break immediately.

It is not a real implementation anymore. It just forwards to the Rust toolkit.

## Supporting Files

- `manual_specs.json`
  - Rust-owned manual seed data converted from the deleted `spec_manual.py`
- `WW.ChartDataAOPA.sqlite`
  - Local airport database used elsewhere in the pipeline and desktop tooling
