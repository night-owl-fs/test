#!/usr/bin/env python3
import argparse
import re
import subprocess
import tempfile
import struct
from collections import defaultdict
from pathlib import Path

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256

AIRPORT_FOLDER_ALIASES = {"KCRQ": "KRCQ"}
SECTION_RE = re.compile(r"^\s*(K[A-Z0-9]{3})\s*$")
HEADER_RE = re.compile(r"\[ZOOM\s*(\d+)\s*[^\]]*?\]")
OUT_Z_RE = re.compile(r"Z(\d+)")
COORD_RE = re.compile(r"\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)")


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


def resolve_gdal_tool(name: str) -> str:
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
    direct = root / airport
    if direct.exists():
        return direct
    alias = AIRPORT_FOLDER_ALIASES.get(airport)
    if alias and (root / alias).exists():
        return root / alias
    return direct


def ensure_worldfile(tile_png: Path, zoom: int):
    pgw = tile_png.with_suffix(".pgw")
    if pgw.exists():
        return
    n = 1 << zoom
    stem = tile_png.stem
    x, y = map(int, stem.split("_"))
    res = WEBM_WORLD / (n * TILE_SIZE)
    minx = -WEBM_HALF + x * TILE_SIZE * res
    maxy = WEBM_HALF - y * TILE_SIZE * res
    top_left_center_x = minx + res / 2.0
    top_left_center_y = maxy - res / 2.0
    content = (
        f"{res:.12f}\n0.000000000000\n0.000000000000\n{-res:.12f}\n"
        f"{top_left_center_x:.12f}\n{top_left_center_y:.12f}\n"
    )
    pgw.write_text(content, encoding="utf-8")


def is_valid_png(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 100:
        return False
    try:
        with path.open("rb") as f:
            sig = f.read(8)
            if sig != b"\x89PNG\r\n\x1a\n":
                return False
            ihdr_len = struct.unpack(">I", f.read(4))[0]
            ctype = f.read(4)
            if ctype != b"IHDR" or ihdr_len < 13:
                return False
            ihdr = f.read(13)
            width, height = struct.unpack(">II", ihdr[:8])
            return width == 256 and height == 256
    except Exception:
        return False


def build_parent_from_children(ap_dir: Path, z: int, x: int, y: int, recurse_depth: int = 0) -> bool:
    if z >= 18:
        return False
    out_dir = ap_dir / str(z)
    out_dir.mkdir(parents=True, exist_ok=True)
    parent = out_dir / f"{x}_{y}.png"
    if is_valid_png(parent):
        ensure_worldfile(parent, z)
        return True

    child_z = z + 1
    cdir = ap_dir / str(child_z)
    if not cdir.exists():
        return False

    children = [
        (2 * x, 2 * y),
        (2 * x + 1, 2 * y),
        (2 * x, 2 * y + 1),
        (2 * x + 1, 2 * y + 1),
    ]
    gdalbuildvrt = resolve_gdal_tool("gdalbuildvrt.exe")
    gdal_translate = resolve_gdal_tool("gdal_translate.exe")
    source_paths = []
    for cx, cy in children:
        cp = cdir / f"{cx}_{cy}.png"
        if not is_valid_png(cp):
            if recurse_depth > 2:
                return False
            if not build_parent_from_children(ap_dir, child_z, cx, cy, recurse_depth + 1):
                return False
            cp = cdir / f"{cx}_{cy}.png"
            if not is_valid_png(cp):
                return False
        source_paths.append(cp)

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
        list_path = Path(f.name)
        for p in source_paths:
            f.write(to_win_path(p) + "\n")
    vrt_path = parent.with_suffix(".tmp.vrt")
    ok, _ = run([gdalbuildvrt, "-q", "-input_file_list", to_win_path(list_path), to_win_path(vrt_path)])
    if not ok:
        list_path.unlink(missing_ok=True)
        return False
    ok, _ = run(
        [
            gdal_translate,
            "-q",
            "-of",
            "PNG",
            "-outsize",
            "256",
            "256",
            to_win_path(vrt_path),
            to_win_path(parent),
        ]
    )
    list_path.unlink(missing_ok=True)
    vrt_path.unlink(missing_ok=True)
    if not ok:
        return False
    ensure_worldfile(parent, z)
    return True


def required_tiles_for_block(base_z: int, out_z: int, x_min: int, x_max: int, y_min: int, y_max: int):
    factor = 1 << (out_z - base_z)
    out = set()
    for by in range(y_min, y_max + 1):
        for bx in range(x_min, x_max + 1):
            x0 = bx * factor
            y0 = by * factor
            for y in range(y0, y0 + factor):
                for x in range(x0, x0 + factor):
                    out.add((x, y))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--index-file", default="CODE/TILE_INDEX_MASTER_KEY 2.txt")
    ap.add_argument("--airport", action="append")
    ap.add_argument("--zoom", action="append", type=int)
    args = ap.parse_args()

    root = Path(args.root).resolve()
    index = (root / args.index_file).resolve()
    sections = parse_tile_index(index)

    airports = sorted(sections.keys())
    if args.airport:
        wanted = {a.upper() for a in args.airport}
        airports = [a for a in airports if a in wanted]

    zoom_filter = set(args.zoom) if args.zoom else None
    fixed = 0
    failed = 0

    for airport in airports:
        ap_dir = resolve_airport_dir(root, airport)
        if not ap_dir.exists():
            continue
        needed_by_zoom = defaultdict(set)
        for b in sections[airport]:
            for oz in b["out_zooms"]:
                if zoom_filter and oz not in zoom_filter:
                    continue
                needed_by_zoom[oz] |= required_tiles_for_block(
                    b["base_z"], oz, b["x_min"], b["x_max"], b["y_min"], b["y_max"]
                )

        for oz, coords in sorted(needed_by_zoom.items()):
            for x, y in sorted(coords):
                p = ap_dir / str(oz) / f"{x}_{y}.png"
                if is_valid_png(p):
                    ensure_worldfile(p, oz)
                    continue
                if build_parent_from_children(ap_dir, oz, x, y):
                    fixed += 1
                    print(f"[FIXED] {airport} Z{oz} {x}_{y}.png")
                else:
                    failed += 1
                    print(f"[MISS] {airport} Z{oz} {x}_{y}.png")

    print(f"\nRepair done. fixed={fixed} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
