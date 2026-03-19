#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
import tempfile
from collections import defaultdict
from pathlib import Path


AIRPORT_FOLDER_ALIASES = {
    "KCRQ": "KRCQ",
}

SECTION_RE = re.compile(r"^\s*(K[A-Z0-9]{3})\s*$")
HEADER_RE = re.compile(r"\[ZOOM\s*(\d+)\s*[^\]]*?\]")
OUT_Z_RE = re.compile(r"Z(\d+)")
COORD_RE = re.compile(r"\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)")
WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256


def run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if p.returncode != 0:
        raise RuntimeError(f"Command failed ({p.returncode}): {' '.join(str(x) for x in cmd)}\n{p.stdout}")
    return p.stdout


def to_win_path(p: Path | str) -> str:
    raw = str(p)
    if raw.startswith("/") and "/mnt/" in raw:
        out = subprocess.run(["wslpath", "-w", raw], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if out.returncode == 0:
            return out.stdout.strip()
    return raw


def resolve_gdal_tool(gdal_bin: str, exe_name: str) -> str:
    base = Path(gdal_bin)
    candidates = [
        base / exe_name,
        Path("/mnt/c/OSGeo4W/bin") / exe_name,
        Path(r"C:\OSGeo4W\bin") / exe_name,
    ]
    for c in candidates:
        if c.exists():
            return str(c)
    return str(base / exe_name)


def ensure_worldfile(tile_png: Path, zoom: int):
    pgw = tile_png.with_suffix(".pgw")
    if pgw.exists():
        return

    png_pgw = tile_png.with_suffix(tile_png.suffix + ".pgw")
    if png_pgw.exists():
        pgw.write_text(png_pgw.read_text(encoding="utf-8", errors="replace"), encoding="utf-8")
        return

    stem = tile_png.stem
    m = re.match(r"^(\d+)_(\d+)$", stem)
    if not m:
        return
    x = int(m.group(1))
    y = int(m.group(2))
    n = 1 << zoom
    res = WEBM_WORLD / (n * TILE_SIZE)
    minx = -WEBM_HALF + x * TILE_SIZE * res
    maxy = WEBM_HALF - y * TILE_SIZE * res
    top_left_center_x = minx + res / 2.0
    top_left_center_y = maxy - res / 2.0
    content = (
        f"{res:.12f}\n"
        f"{0.0:.12f}\n"
        f"{0.0:.12f}\n"
        f"{-res:.12f}\n"
        f"{top_left_center_x:.12f}\n"
        f"{top_left_center_y:.12f}\n"
    )
    pgw.write_text(content, encoding="utf-8")


def parse_tile_index(index_path: Path):
    data = index_path.read_text(encoding="utf-8", errors="replace").splitlines()
    sections = defaultdict(list)

    current_airport = None
    i = 0
    while i < len(data):
        line = data[i].strip()
        m_section = SECTION_RE.match(line)
        if m_section:
            current_airport = m_section.group(1)
            i += 1
            continue

        if current_airport:
            m_header = HEADER_RE.search(line)
            if m_header:
                base_z = int(m_header.group(1))
                out_zooms = [int(z) for z in OUT_Z_RE.findall(line)]
                coords = []
                j = i + 1
                while j < len(data):
                    row = data[j].strip()
                    if not row:
                        j += 1
                        continue
                    if row.startswith("[ZOOM") or SECTION_RE.match(row):
                        break
                    if row.upper().startswith("CENTER"):
                        j += 1
                        continue
                    coords.extend((int(x), int(y)) for (x, y) in COORD_RE.findall(row))
                    j += 1
                if coords and out_zooms:
                    xs = [x for x, _ in coords]
                    ys = [y for _, y in coords]
                    x_min, x_max = min(xs), max(xs)
                    y_min, y_max = min(ys), max(ys)
                    sections[current_airport].append(
                        {
                            "base_z": base_z,
                            "out_zooms": out_zooms,
                            "x_min": x_min,
                            "x_max": x_max,
                            "y_min": y_min,
                            "y_max": y_max,
                        }
                    )
                i = j
                continue
        i += 1

    return sections


def build_one_cone(gdalbuildvrt, gdal_translate, tile_dir: Path, out_dir: Path, base_z: int, out_z: int, bx: int, by: int, r: int, c: int):
    factor = 1 << (out_z - base_z)
    x0 = bx * factor
    y0 = by * factor
    expected = factor * factor

    src_tiles = []
    for yy in range(y0, y0 + factor):
        for xx in range(x0, x0 + factor):
            p = tile_dir / f"{xx}_{yy}.png"
            if not p.exists():
                return False, f"missing tile {p.name}"
            ensure_worldfile(p, out_z)
            src_tiles.append(p.resolve())

    if len(src_tiles) != expected:
        return False, f"expected {expected} tiles, found {len(src_tiles)}"

    stem = f"Z{out_z}_fromZ{base_z}_r{r}_c{c}_baseX{bx}_baseY{by}"
    vrt_path = out_dir / f"{stem}.vrt"
    tif_path = out_dir / f"{stem}.tif"
    if tif_path.exists() and vrt_path.exists():
        return True, str(tif_path)

    list_path = None
    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
        list_path = Path(f.name)
        for p in src_tiles:
            f.write(to_win_path(p) + "\n")

    try:
        run([gdalbuildvrt, "-input_file_list", to_win_path(list_path), to_win_path(vrt_path)])
        try:
            run(
                [
                    gdal_translate,
                    "-of",
                    "GTiff",
                    "-a_srs",
                    "EPSG:3857",
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
                    to_win_path(vrt_path),
                    to_win_path(tif_path),
                ]
            )
        except Exception as exc:
            return False, f"gdal_translate failed: {exc}"
    finally:
        if list_path and list_path.exists():
            list_path.unlink()

    return True, str(tif_path)


def resolve_airport_dir(root: Path, airport: str) -> Path:
    direct = root / airport
    if direct.exists():
        return direct
    alias = AIRPORT_FOLDER_ALIASES.get(airport)
    if alias:
        apath = root / alias
        if apath.exists():
            return apath
    return direct


def main():
    ap = argparse.ArgumentParser(description="Build all cone GeoTIFFs from TILE_INDEX_MASTER_KEY file.")
    ap.add_argument("--root", default=".", help="Project root with K### folders.")
    ap.add_argument("--index-file", default="CODE/TILE_INDEX_MASTER_KEY 2.txt")
    ap.add_argument("--out-root", default="CODE/CONE_GEOTIFF_OUTPUT")
    ap.add_argument("--gdal-bin", default="/mnt/c/OSGeo4W/bin")
    ap.add_argument("--airport", action="append", help="Optional airport filter (e.g., KASE).")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    index_file = (root / args.index_file).resolve()
    out_root = (root / args.out_root).resolve()
    out_root.mkdir(parents=True, exist_ok=True)

    gdalbuildvrt = resolve_gdal_tool(args.gdal_bin, "gdalbuildvrt.exe")
    gdal_translate = resolve_gdal_tool(args.gdal_bin, "gdal_translate.exe")

    sections = parse_tile_index(index_file)
    airports = sorted(sections.keys())
    if args.airport:
        wanted = {x.upper() for x in args.airport}
        airports = [a for a in airports if a in wanted]

    if not airports:
        print("No matching airports found in index file.")
        return 1

    total_built = 0
    total_skipped = 0
    warnings = []

    for airport in airports:
        airport_dir = resolve_airport_dir(root, airport)
        if not airport_dir.exists():
            warnings.append(f"{airport}: airport folder not found")
            continue

        airport_out = out_root / airport
        airport_out.mkdir(parents=True, exist_ok=True)
        per_zoom_tifs = defaultdict(list)

        for block in sections[airport]:
            base_z = block["base_z"]
            x_min, x_max = block["x_min"], block["x_max"]
            y_min, y_max = block["y_min"], block["y_max"]

            grid_w = x_max - x_min + 1
            grid_h = y_max - y_min + 1

            for out_z in block["out_zooms"]:
                tile_dir = airport_dir / str(out_z)
                if not tile_dir.exists():
                    warnings.append(f"{airport} Z{out_z}: tile folder missing ({tile_dir})")
                    continue

                out_dir = airport_out / f"Z{out_z}"
                out_dir.mkdir(parents=True, exist_ok=True)

                for r, by in enumerate(range(y_min, y_max + 1)):
                    for c, bx in enumerate(range(x_min, x_max + 1)):
                        ok, msg = build_one_cone(
                            gdalbuildvrt=gdalbuildvrt,
                            gdal_translate=gdal_translate,
                            tile_dir=tile_dir,
                            out_dir=out_dir,
                            base_z=base_z,
                            out_z=out_z,
                            bx=bx,
                            by=by,
                            r=r,
                            c=c,
                        )
                        if ok:
                            total_built += 1
                            per_zoom_tifs[out_z].append(msg)
                            print(f"[OK] {airport} Z{out_z} ({r+1},{c+1}/{grid_h},{grid_w})", flush=True)
                        else:
                            total_skipped += 1
                            warnings.append(f"{airport} Z{out_z} r{r} c{c}: {msg}")

        for out_z, tifs in per_zoom_tifs.items():
            if not tifs:
                continue
            list_path = airport_out / f"z{out_z}_list.txt"
            vrt_path = airport_out / f"z{out_z}.vrt"
            list_path.write_text("".join(to_win_path(Path(p).resolve()) + "\n" for p in tifs), encoding="utf-8")
            run([gdalbuildvrt, "-input_file_list", to_win_path(list_path), to_win_path(vrt_path)])

    print(f"\nDone. Built {total_built} GeoTIFFs. Skipped {total_skipped}.", flush=True)
    if warnings:
        print("\nWarnings:", flush=True)
        for w in warnings:
            print(f"- {w}", flush=True)

    return 0 if total_built > 0 else 2


if __name__ == "__main__":
    sys.exit(main())
