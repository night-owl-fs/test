**Generate GeoTIFFs & VRTs from specs**

- Prerequisites:
  - Install GDAL (make sure `gdalbuildvrt`, `gdal_translate`, and `gdaladdo` are on PATH).
  - Generate the spec JSON files first with the Rust step-1 toolkit.

- Rust step-1 spec generation:

```bash
cargo run -p step_airport_indexer --bin airport_index_toolkit -- \
  generate-specs \
  --source index \
  --index-file crates/1_step_airport_indexer/TILE_INDEX_MASTER_KEY_fixed_kmwl.txt \
  --out-dir crates/5_step_geotiff_to_heaven/specs
```

You can also use `--source manual` with `crates/1_step_airport_indexer/manual_specs.json` if you want the checked-in manual layouts instead of rebuilding from the tile index.

- Script: `generate_geotiffs_from_specs.ps1` (PowerShell)

Example: run from PowerShell in Administrator or developer shell

```powershell
# change to project folder
Set-Location 'C:\Users\Ravery\Desktop\AIRPORTS\CODE\brcj_pgw_geotiff'

# run script (default root already points to AIRPORTS)
.\generate_geotiffs_from_specs.ps1 -CreateOverviews

# or explicitly pass root and output folder
.\generate_geotiffs_from_specs.ps1 -Root 'C:\Users\Ravery\Desktop\AIRPORTS' -SpecsDir 'CODE\brcj_pgw_geotiff\specs' -OutDir 'CODE\brcj_pgw_geotiff\output' -CreateOverviews
```

Where outputs will be written:
- `CODE\brcj_pgw_geotiff\output\<AIRPORT>\<group>.tif` (and `<group>.vrt`)

Notes:
- The script expects tiles named like `1234_5678.png` under the BRCJ zoom folder: `KASE_BRCJ\18\1234_5678.png`.
- The PNG world files (`.pgw`) must be present next to the PNGs for correct georeferencing. GDAL will read them automatically when building VRTs.
- If some tiles are missing, the script will warn and skip them; groups with no existing tiles will be skipped entirely.

If you want, I can:
- Run the Rust spec generator to refresh specs from the index file.
- Run the PowerShell script (if GDAL is installed on this machine) to create GeoTIFFs and VRTs now.
