#!/usr/bin/env python3
import argparse
import re
import subprocess
import tempfile
import math
from collections import defaultdict
from pathlib import Path

AIRPORT_FOLDER_ALIASES = {"KCRQ": "KRCQ"}
SECTION_RE = re.compile(r"^\s*(K[A-Z0-9]{3})\s*$")
HEADER_RE = re.compile(r"\[ZOOM\s*(\d+)\s*[^\]]*?\]")
OUT_Z_RE = re.compile(r"Z(\d+)")
COORD_RE = re.compile(r"\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)")
EXPECTED = {"Z13": 9, "Z14": 9, "Z15": 9, "Z16": 9, "Z17": 25, "Z18": 9}


def run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode == 0, p.stdout


def to_win_path(p: Path | str) -> str:
    s = str(p)
    if s.startswith("/"):
        out = subprocess.run(["wslpath", "-w", s], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if out.returncode == 0:
            return out.stdout.strip()
    return s


def resolve_gdal(name: str) -> str:
    for base in (Path("/mnt/c/OSGeo4W/bin"), Path(r"C:\OSGeo4W\bin")):
        p = base / name
        if p.exists():
            return str(p)
    return name


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
                    sections[current_airport].append(
                        {
                            "base_z": base_z,
                            "out_zooms": out_zooms,
                            "x_min": min(xs),
                            "x_max": max(xs),
                            "y_min": min(ys),
                            "y_max": max(ys),
                        }
                    )
                i = j
                continue
        i += 1
    return sections


def resolve_airport_dir(root: Path, airport: str) -> Path:
    d = root / airport
    if d.exists():
        return d
    alias = AIRPORT_FOLDER_ALIASES.get(airport)
    if alias and (root / alias).exists():
        return root / alias
    return d


WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0


def valid_png(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 100


def cone_bounds(out_z: int, base_z: int, bx: int, by: int):
    factor = 1 << (out_z - base_z)
    n = 1 << out_z
    tile_extent = WEBM_WORLD / n
    x0 = bx * factor
    y0 = by * factor
    minx = -WEBM_HALF + x0 * tile_extent
    maxx = minx + factor * tile_extent
    maxy = WEBM_HALF - y0 * tile_extent
    miny = maxy - factor * tile_extent
    return minx, miny, maxx, maxy, factor


def source_tile_range(minx: float, miny: float, maxx: float, maxy: float, zsrc: int):
    n = 1 << zsrc
    tile_extent = WEBM_WORLD / n
    x_min = int(math.floor((minx + WEBM_HALF) / tile_extent))
    x_max = int(math.ceil((maxx + WEBM_HALF) / tile_extent) - 1)
    y_min = int(math.floor((WEBM_HALF - maxy) / tile_extent))
    y_max = int(math.ceil((WEBM_HALF - miny) / tile_extent) - 1)
    x_min = max(0, min(n - 1, x_min))
    x_max = max(0, min(n - 1, x_max))
    y_min = max(0, min(n - 1, y_min))
    y_max = max(0, min(n - 1, y_max))
    return x_min, x_max, y_min, y_max


def try_build_from_source_zoom(
    airport_dir: Path,
    out_tif: Path,
    out_vrt: Path,
    base_z: int,
    out_z: int,
    bx: int,
    by: int,
    src_z: int,
):
    src_dir = airport_dir / str(src_z)
    if not src_dir.exists():
        return False, f"source zoom folder missing {src_dir}"

    minx, miny, maxx, maxy, factor = cone_bounds(out_z, base_z, bx, by)
    x_min, x_max, y_min, y_max = source_tile_range(minx, miny, maxx, maxy, src_z)

    sources = []
    source_count = (x_max - x_min + 1) * (y_max - y_min + 1)
    if source_count <= 0:
        return False, "no source tiles in computed range"

    sources = []
    for y in range(y_min, y_max + 1):
        for x in range(x_min, x_max + 1):
            p = src_dir / f"{x}_{y}.png"
            if not valid_png(p):
                return False, f"missing/corrupt source tile {p.name}"
            sources.append(p)

    gdalbuildvrt = resolve_gdal("gdalbuildvrt.exe")
    gdal_translate = resolve_gdal("gdal_translate.exe")
    output_px = factor * 256

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
        list_path = Path(f.name)
        for p in sources:
            f.write(to_win_path(p) + "\n")

    ok, out = run([gdalbuildvrt, "-q", "-input_file_list", to_win_path(list_path), to_win_path(out_vrt)])
    list_path.unlink(missing_ok=True)
    if not ok:
        return False, out

    ok, out = run(
        [
            gdal_translate,
            "-q",
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
            "-projwin",
            f"{minx:.15f}",
            f"{maxy:.15f}",
            f"{maxx:.15f}",
            f"{miny:.15f}",
            "-outsize",
            str(output_px),
            str(output_px),
            to_win_path(out_vrt),
            to_win_path(out_tif),
        ]
    )
    return ok, out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--index-file", default="CODE/TILE_INDEX_MASTER_KEY 2.txt")
    ap.add_argument("--out-root", default="CODE/CONE_GEOTIFF_OUTPUT")
    ap.add_argument("--airport", action="append")
    ap.add_argument("--zoom", action="append", type=int)
    args = ap.parse_args()

    root = Path(args.root).resolve()
    out_root = (root / args.out_root).resolve()
    sections = parse_tile_index((root / args.index_file).resolve())
    zoom_filter = set(args.zoom) if args.zoom else None

    airports = sorted(sections.keys())
    if args.airport:
        want = {a.upper() for a in args.airport}
        airports = [a for a in airports if a in want]

    built = 0
    skipped = 0
    failed = 0

    for airport in airports:
        airport_dir = resolve_airport_dir(root, airport)
        airport_out = out_root / airport
        if not airport_out.exists():
            continue

        for block in sections[airport]:
            base_z = block["base_z"]
            for out_z in block["out_zooms"]:
                if zoom_filter and out_z not in zoom_filter:
                    continue
                zdir = airport_out / f"Z{out_z}"
                zdir.mkdir(parents=True, exist_ok=True)

                for r, by in enumerate(range(block["y_min"], block["y_max"] + 1)):
                    for c, bx in enumerate(range(block["x_min"], block["x_max"] + 1)):
                        stem = f"Z{out_z}_fromZ{base_z}_r{r}_c{c}_baseX{bx}_baseY{by}"
                        tif = zdir / f"{stem}.tif"
                        vrt = zdir / f"{stem}.vrt"
                        if tif.exists():
                            skipped += 1
                            continue

                        success = False
                        available_src_zooms = []
                        for zsrc in range(13, 19):
                            if zsrc == out_z:
                                continue
                            if (airport_dir / str(zsrc)).exists():
                                available_src_zooms.append(zsrc)
                        available_src_zooms.sort(key=lambda zsrc: (abs(zsrc - out_z), 0 if zsrc > out_z else 1))

                        for src_z in available_src_zooms:
                            ok, msg = try_build_from_source_zoom(
                                airport_dir=airport_dir,
                                out_tif=tif,
                                out_vrt=vrt,
                                base_z=base_z,
                                out_z=out_z,
                                bx=bx,
                                by=by,
                                src_z=src_z,
                            )
                            if ok:
                                print(f"[FILL] {airport} Z{out_z} r{r}c{c} from Z{src_z}")
                                built += 1
                                success = True
                                break
                        if not success:
                            failed += 1
                            print(f"[FAIL] {airport} Z{out_z} r{r}c{c}")

    print(f"\nDone. filled={built} skipped_existing={skipped} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
