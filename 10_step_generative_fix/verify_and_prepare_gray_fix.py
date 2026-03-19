#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
from collections import Counter
from pathlib import Path

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
FNAME_RE = re.compile(r"Z(?P<out>\d+)_fromZ(?P<base>\d+)_r(?P<r>\d+)_c(?P<c>\d+)_baseX(?P<bx>\d+)_baseY(?P<by>\d+)\.tif$")

TARGETS = [
    "CODE/CONE_GEOTIFF_OUTPUT/KJFK/Z14/Z14_fromZ9_r2_c1_baseX151_baseY193.tif",
    "CODE/CONE_GEOTIFF_OUTPUT/KJFK/Z14/Z14_fromZ9_r2_c2_baseX152_baseY193.tif",
    "CODE/CONE_GEOTIFF_OUTPUT/KJFK/Z15/Z15_fromZ9_r2_c1_baseX151_baseY193.tif",
    "CODE/CONE_GEOTIFF_OUTPUT/KJFK/Z15/Z15_fromZ9_r2_c2_baseX152_baseY193.tif",
    "CODE/CONE_GEOTIFF_OUTPUT/KLGA/Z14/Z14_fromZ9_r2_c2_baseX151_baseY193.tif",
]


def wpath(p: Path) -> str:
    s = str(p)
    if s.startswith("/mnt/"):
        out = subprocess.run(["wslpath", "-w", s], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if out.returncode == 0:
            return out.stdout.strip()
    return s


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
    return minx, miny, maxx, maxy


def source_range(minx: float, miny: float, maxx: float, maxy: float, zsrc: int):
    n = 1 << zsrc
    tile_extent = WEBM_WORLD / n
    x_min = int(math.floor((minx + WEBM_HALF) / tile_extent))
    x_max = int(math.ceil((maxx + WEBM_HALF) / tile_extent) - 1)
    y_min = int(math.floor((WEBM_HALF - maxy) / tile_extent))
    y_max = int(math.ceil((WEBM_HALF - miny) / tile_extent) - 1)
    return x_min, x_max, y_min, y_max


def is_png(path: Path) -> bool:
    if not path.exists() or path.stat().st_size < 100:
        return False
    with path.open("rb") as f:
        return f.read(8) == b"\x89PNG\r\n\x1a\n"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def gdal_stats(gdalinfo: str, path: Path):
    cp = subprocess.run([gdalinfo, "-json", "-stats", wpath(path)], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    if cp.returncode != 0:
        return None
    try:
        j = json.loads(cp.stdout)
        bands = (j.get("bands") or [])[:3]
        if len(bands) < 3:
            return None
        means = []
        stds = []
        mins = []
        maxs = []
        for b in bands:
            md = (b.get("metadata") or {}).get("", {})
            means.append(float(md.get("STATISTICS_MEAN", "0")))
            stds.append(float(md.get("STATISTICS_STDDEV", "0")))
            mins.append(float(md.get("STATISTICS_MINIMUM", "0")))
            maxs.append(float(md.get("STATISTICS_MAXIMUM", "0")))
        return {
            "avg_mean": sum(means) / 3,
            "avg_std": sum(stds) / 3,
            "ch_delta": max(means) - min(means),
            "avg_dyn": sum((maxs[i] - mins[i]) for i in range(3)) / 3,
        }
    except Exception:
        return None


def main() -> int:
    ap = argparse.ArgumentParser(description="Verify source tiles for unresolved gray cones and emit exact redownload lists.")
    ap.add_argument("--root", default=r"C:\Users\ravery\Downloads\AIRPORT_TILES")
    ap.add_argument("--report", default=r"C:\Users\ravery\Downloads\AIRPORT_TILES\CODE\reports\gray_fix_precheck_report.md")
    ap.add_argument("--download-list", default=r"C:\Users\ravery\Downloads\AIRPORT_TILES\CODE\reports\gray_fix_tiles_to_redownload.txt")
    args = ap.parse_args()

    root = Path(args.root)
    gdalinfo = "/mnt/c/OSGeo4W/bin/gdalinfo.exe"

    bad_tiles = set()
    report_lines = ["# Gray Fix Precheck", ""]

    for target in TARGETS:
        t = (root / target).resolve()
        m = FNAME_RE.search(t.name)
        if not m:
            report_lines.append(f"- FAIL parse target name: {t}")
            continue

        out_z = int(m.group("out"))
        base_z = int(m.group("base"))
        bx = int(m.group("bx"))
        by = int(m.group("by"))
        airport = t.parent.parent.name

        minx, miny, maxx, maxy = cone_bounds(out_z, base_z, bx, by)
        x0, x1, y0, y1 = source_range(minx, miny, maxx, maxy, out_z)

        tiles = []
        missing = 0
        invalid = 0
        for x in range(x0, x1 + 1):
            for y in range(y0, y1 + 1):
                p = root / airport / str(out_z) / f"{x}_{y}.png"
                tiles.append(p)
                if not p.exists():
                    missing += 1
                    bad_tiles.add((airport, out_z, x, y))
                elif not is_png(p):
                    invalid += 1
                    bad_tiles.add((airport, out_z, x, y))

        # duplicate-content test (strong placeholder signal)
        hash_counter = Counter()
        sampled = 0
        for p in tiles:
            if p.exists() and is_png(p):
                sampled += 1
                try:
                    hash_counter[sha256(p)] += 1
                except Exception:
                    pass

        top_dup = hash_counter.most_common(1)[0][1] if hash_counter else 0
        dup_ratio = (top_dup / sampled) if sampled else 0.0

        # sample tile stats across footprint corners/center
        stats_bad = 0
        sample_pts = [(x0, y0), ((x0 + x1) // 2, (y0 + y1) // 2), (x1, y1)]
        for sx, sy in sample_pts:
            sp = root / airport / str(out_z) / f"{sx}_{sy}.png"
            if sp.exists() and is_png(sp):
                st = gdal_stats(gdalinfo, sp)
                if st and st["avg_std"] < 6 and 120 <= st["avg_mean"] <= 150 and st["ch_delta"] < 1.0:
                    stats_bad += 1

        flagged_placeholder = dup_ratio > 0.80 or stats_bad >= 2

        if flagged_placeholder:
            for x in range(x0, x1 + 1):
                for y in range(y0, y1 + 1):
                    bad_tiles.add((airport, out_z, x, y))

        report_lines.append(f"## {airport} {t.name}")
        report_lines.append(f"- source zoom: {out_z}")
        report_lines.append(f"- required tiles: {(x1-x0+1)*(y1-y0+1)} ({x0}..{x1}, {y0}..{y1})")
        report_lines.append(f"- missing: {missing}")
        report_lines.append(f"- invalid_png: {invalid}")
        report_lines.append(f"- duplicate_ratio_top_hash: {dup_ratio:.3f}")
        report_lines.append(f"- placeholder_signature_hits: {stats_bad}/3")
        report_lines.append(f"- flagged_placeholder: {'YES' if flagged_placeholder else 'NO'}")
        report_lines.append("")

    report_path = Path(args.report)
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text("\n".join(report_lines), encoding="utf-8")

    dl_path = Path(args.download_list)
    dl_path.parent.mkdir(parents=True, exist_ok=True)
    with dl_path.open("w", encoding="utf-8") as f:
        for ap_name, z, x, y in sorted(bad_tiles):
            f.write(f"{ap_name}/{z}/{x}_{y}.png\n")

    print(f"report: {report_path}")
    print(f"download_list: {dl_path}")
    print(f"tiles_to_replace: {len(bad_tiles)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
