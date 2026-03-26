# Step 8 Merge Tiles

## Goal

`merge_tiles_rust` is the Rust-first merge stage for raster tile SQLite databases.

It replaces the primary production use of `merge_tiles_v2.py` and keeps Python only as a fallback/reference path.

## What The Rust Merge Does

- Merges two or more tile databases with last-file-wins behavior.
- Handles SQLite/GeoPackage tile tables.
- Optionally removes background-only tiles from the primary and secondary inputs during merge.
- Refreshes GeoPackage metadata from the final merged output.
- Refreshes MBTiles min/max zoom and bounds metadata.
- Can optionally run `gdaladdo` overviews for GeoPackage outputs.

## Default Rust Command

```bash
cargo run -p merge_tiles_rust -- \
  --inputs C:\\path\\imagery.sqlite C:\\path\\imagery_trim.sqlite \
  --output C:\\path\\imagery_merged.sqlite \
  --force
```

## Key Flags

- `--skip-background`
  - Enable background-only tile filtering.
- `--background-rgb`
  - RGB color treated as background.
- `--threshold`
  - Per-pixel color distance tolerance for background matching.
- `--percentage`
  - Required fraction of matching pixels before a tile is treated as background.
- `--variance-threshold`
  - Uniformity guard so only flat tiles are skipped.
- `--add-overviews`
  - Runs `gdaladdo` for GeoPackage outputs after merge.

## Rust-First Policy

- Default path: `merge_tiles_rust`
- Fallback path: `merge_tiles_v2.py`

The main pipeline now runs step 8 as Rust-first and automatically retries the Python fallback only if the Rust merge fails.
