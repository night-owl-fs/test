# merge_rust

## Goal
Rust replacement for `merge_tiles_v2.py` that merges multiple trimmed GeoPackage tile databases into one, with deterministic overlap handling.

## Scope
- Input: multiple `.sqlite` GeoPackage files.
- Output: one merged `.sqlite` file.
- Rule: later input wins on tile key conflicts (`zoom_level`, `tile_column`, `tile_row`).
- Preserve/update GeoPackage metadata tables (`gpkg_contents`, `gpkg_tile_matrix`, `gpkg_tile_matrix_set`).

## CLI Proposal
```bash
merge_rust \
  --inputs C:\\path\\KASE_trim.sqlite C:\\path\\KCRQ_trim.sqlite ... \
  --output C:\\path\\ALL_AIRPORTS_Z18_no_overlap_trim_merged.sqlite \
  --force \
  --background-rgb 255,255,255 \
  --skip-background true
```

## Required Behavior
- Copy first DB as base.
- For each additional DB:
  - iterate tile rows
  - optional background-tile reject
  - `INSERT OR REPLACE` into target tiles table
- Recompute union bounds and zoom range.
- Write final metadata updates.

## Recommended Crates
- `rusqlite`
- `image`
- `clap`
- `anyhow`

## Status
- Current production path still uses `merge_tiles_v2.py`.
- This doc is the implementation contract for Rust migration.
