# trim_rust

## Goal
Rust replacement for `adios_mfer_trim_job.py` that converts GeoPackage tile blobs from PNG to JPG while preserving schema/metadata.

## Scope
- Input: one `.sqlite`/GeoPackage tile DB.
- Output: one trimmed `.sqlite` DB.
- Convert `tiles.tile_data` PNG -> JPG.
- Keep `zoom_level`, `tile_column`, `tile_row` unchanged.
- Copy non-tile tables as-is.
- Skip internal SQLite objects (for example `sqlite_sequence`).

## CLI Proposal
```bash
trim_rust \
  --input C:\\path\\KASE_Z18_no_overlap.sqlite \
  --output C:\\path\\KASE_Z18_no_overlap_trim.sqlite \
  --quality 90 \
  --background 0,0,0 \
  --min-zoom 6 \
  --max-zoom 19 \
  --force
```

## Required Behavior
- Validate input exists and output overwrite policy.
- Detect tile table(s) by required columns:
  - `zoom_level`, `tile_column`, `tile_row`, `tile_data`
- Batch insert for speed.
- Print progress with rows/sec and ETA.
- Return non-zero on conversion failure.

## Recommended Crates
- `rusqlite`
- `image`
- `clap`
- `anyhow`

## Status
- Current production path still uses `adios_mfer_trim_job.py`.
- This doc is the build spec for a Rust parity implementation.
