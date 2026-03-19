# cone_to_heaven_rust (GeoTIFF builder)

Single-job BASE->OUT GeoTIFF builder for NighthawkFS cone layout.

## Build

```bash
cargo build --release
```

## Run (example)

```bash
target/release/cone_to_heaven_rust   --tiles-root "/path/to/Z18_BRCJ"   --area-name "KSAN"   --center-lat 32.733556   --truth-crs EPSG:32611   --truth-res 0.5   --chunk "KSAN"   --out-root "/path/to/_GEOTIFF_BUILD/KSAN/Z18"   --base-z 13   --base-anchor "1428:3305"   --grid 5   --out-z 18   --scheme xyz   --ext ".png"   --compress deflate   --build-overviews   --verify
```
