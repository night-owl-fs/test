**Generate GeoTIFFs & VRTs from specs**

- Prerequisites:
  - Install GDAL (make sure `gdalbuildvrt`, `gdal_translate`, and `gdaladdo` are on PATH).
  - Specs generated into `CODE\brcj_pgw_geotiff\specs` (the existing `spec_generator*.py` produce these).

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
- Run the spec generators to refresh specs from the INDEX files.
- Run the PowerShell script (if GDAL is installed on this machine) to create GeoTIFFs and VRTs now.
