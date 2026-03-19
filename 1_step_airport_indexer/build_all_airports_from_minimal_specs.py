#!/usr/bin/env python3
import argparse
import json
import subprocess
import sys
from pathlib import Path


AIRPORT_ALIASES = {
    "KRCQ": "KCRQ",
}

ZOOMS = [13, 14, 15, 16, 17, 18]


def run(cmd):
    proc = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({proc.returncode}): {' '.join(str(c) for c in cmd)}\n{proc.stdout}")
    return proc.stdout


def resolve_spec_file(specs_dir: Path, airport: str, zoom: int) -> Path | None:
    candidates = [airport]
    if airport in AIRPORT_ALIASES:
        candidates.append(AIRPORT_ALIASES[airport])
    for c in candidates:
        p = specs_dir / f"{c}_Z{zoom}_MINIMAL_spec.json"
        if p.exists():
            return p
    return None


def existing_tiles(tile_dir: Path, files: list[str]) -> list[Path]:
    out = []
    seen = set()
    for name in files:
        if name in seen:
            continue
        seen.add(name)
        p = tile_dir / name
        if p.exists():
            out.append(p.resolve())
    return out


def build_group(gdalbuildvrt: str, gdal_translate: str, tiles: list[Path], vrt_path: Path, tif_path: Path):
    list_path = vrt_path.with_suffix(".txt")
    list_path.write_text("".join(f"{p.as_posix()}\n" for p in tiles), encoding="utf-8")
    try:
        run([gdalbuildvrt, "-input_file_list", str(list_path), str(vrt_path)])
        run(
            [
                gdal_translate,
                "-of",
                "GTiff",
                "-co",
                "TILED=YES",
                "-co",
                "COMPRESS=DEFLATE",
                "-co",
                "PREDICTOR=2",
                "-co",
                "ZLEVEL=6",
                "-co",
                "BIGTIFF=IF_SAFER",
                str(vrt_path),
                str(tif_path),
            ]
        )
    finally:
        if list_path.exists():
            list_path.unlink()


def discover_airports(root: Path) -> list[str]:
    return sorted(p.name for p in root.iterdir() if p.is_dir() and p.name.startswith("K") and len(p.name) == 4)


def main():
    ap = argparse.ArgumentParser(description="Build GeoTIFFs for all K### airports from minimal specs.")
    ap.add_argument("--root", default=".", help="Project root (contains K### folders and CODE/specs).")
    ap.add_argument("--specs-dir", default="CODE/specs", help="Directory with *_Z##_MINIMAL_spec.json.")
    ap.add_argument("--out-dir", default="CODE/GEOTIFF_OUTPUT", help="Output root for generated VRT/TIFF files.")
    ap.add_argument("--gdal-bin", default=r"C:\OSGeo4W\bin", help="GDAL binary folder (contains gdalbuildvrt.exe and gdal_translate.exe).")
    ap.add_argument("--airport", action="append", help="Optional airport filter; can be passed multiple times.")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    specs_dir = (root / args.specs_dir).resolve()
    out_root = (root / args.out_dir).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    gdalbuildvrt = str(Path(args.gdal_bin) / "gdalbuildvrt.exe")
    gdal_translate = str(Path(args.gdal_bin) / "gdal_translate.exe")

    airports = sorted(set(a.upper() for a in args.airport)) if args.airport else discover_airports(root)
    if not airports:
        print("No airport folders found.")
        return 1

    built = 0
    skipped_groups = 0
    warnings = []

    for airport in airports:
        airport_dir = root / airport
        if not airport_dir.exists():
            warnings.append(f"{airport}: folder missing")
            continue

        for zoom in ZOOMS:
            tile_dir = airport_dir / str(zoom)
            if not tile_dir.exists():
                warnings.append(f"{airport} Z{zoom}: tile folder missing ({tile_dir})")
                continue

            spec_file = resolve_spec_file(specs_dir, airport, zoom)
            if not spec_file:
                warnings.append(f"{airport} Z{zoom}: spec missing")
                continue

            try:
                spec = json.loads(spec_file.read_text(encoding="utf-8"))
            except Exception as exc:
                warnings.append(f"{airport} Z{zoom}: cannot parse spec ({spec_file.name}): {exc}")
                continue

            groups = spec.get("groups", [])
            if not groups:
                warnings.append(f"{airport} Z{zoom}: no groups in spec {spec_file.name}")
                continue

            out_dir = out_root / airport / f"Z{zoom}"
            out_dir.mkdir(parents=True, exist_ok=True)

            for group in groups:
                group_name = group.get("name", f"{airport}_Z{zoom}_GROUP")
                output_tiff = group.get("output_tiff", f"{group_name}.tif")
                file_list = group.get("files", [])
                if not file_list:
                    skipped_groups += 1
                    warnings.append(f"{airport} Z{zoom} {group_name}: empty file list")
                    continue

                tiles = existing_tiles(tile_dir, file_list)
                if not tiles:
                    skipped_groups += 1
                    warnings.append(f"{airport} Z{zoom} {group_name}: no matching tiles found in {tile_dir}")
                    continue

                vrt_path = out_dir / f"{group_name}.vrt"
                tif_path = out_dir / output_tiff

                print(f"[BUILD] {airport} Z{zoom} {group_name}: {len(tiles)} tiles")
                try:
                    build_group(gdalbuildvrt, gdal_translate, tiles, vrt_path, tif_path)
                except Exception as exc:
                    skipped_groups += 1
                    warnings.append(f"{airport} Z{zoom} {group_name}: build failed: {exc}")
                    continue

                built += 1

    print(f"\nDone. Built groups: {built}, skipped groups: {skipped_groups}")
    if warnings:
        print("\nWarnings:")
        for w in warnings:
            print(f"- {w}")

    return 0 if built > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
