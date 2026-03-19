#!/usr/bin/env python3
"""
create_pgw_sidecars.py
- NO PIL, never opens images
- Computes PGW worldfiles from z/x/y in file path
- Logs missing/empty/bad_path separately
- Works with folder layouts like:
    .../Z18/R000_C000/18/137728/89856.png
    .../18/137728/89856.png
    .../Z18/18/137728/89856.png

python -u create_pgw_sidecars.py \
  --root "/Volumes/T9/TILES_TANK/BADEN_OUTER_PINK_brcj" \
  --ext ".png" \
  --workers 20 \
  --overwrite

World file for EPSG:3857 Web Mercator:
A = +pixel_size
D = 0
B = 0
E = -pixel_size   (NEGATIVE!)
C = x_min + pixel_size/2
F = y_max - pixel_size/2
"""

from __future__ import annotations
import argparse
import concurrent.futures as cf
import os
import re
from pathlib import Path
from typing import Optional, Tuple

ORIGIN_SHIFT = 20037508.342789244  # meters
TILE_SIZE = 256

# Match .../<z>/<x>/<y>.<ext> at end of path
XYZ_RE = re.compile(r"/(?P<z>\d{1,2})/(?P<x>\d+)/(?P<y>\d+)\.(?P<ext>[A-Za-z0-9]+)$")


def pixel_size_m(z: int) -> float:
    # meters per pixel at this zoom for WebMercator at equator
    # initial resolution = 2*ORIGIN_SHIFT / TILE_SIZE
    return (2.0 * ORIGIN_SHIFT) / (TILE_SIZE * (2 ** z))


def tile_bounds_merc(z: int, x: int, y: int) -> Tuple[float, float, float, float]:
    """
    Returns (xmin, ymin, xmax, ymax) in EPSG:3857 meters for the tile z/x/y
    Using XYZ scheme: y increases downward.
    """
    n = 2 ** z
    tile_span = (2.0 * ORIGIN_SHIFT) / n

    xmin = -ORIGIN_SHIFT + x * tile_span
    xmax = xmin + tile_span

    ymax = ORIGIN_SHIFT - y * tile_span
    ymin = ymax - tile_span
    return xmin, ymin, xmax, ymax


def compute_pgw_lines(z: int, x: int, y: int) -> str:
    px = pixel_size_m(z)
    xmin, ymin, xmax, ymax = tile_bounds_merc(z, x, y)

    # Worldfile affine params
    A = px
    D = 0.0
    B = 0.0
    E = -px  # IMPORTANT: negative for north-up images in WebMercator
    C = xmin + px / 2.0
    F = ymax - px / 2.0

    return "\n".join(f"{v:.12f}" for v in (A, D, B, E, C, F)) + "\n"


def parse_xyz(path: Path) -> Optional[Tuple[int, int, int]]:
    s = path.as_posix()
    m = XYZ_RE.search(s)
    if not m:
        return None
    return int(m.group("z")), int(m.group("x")), int(m.group("y"))


def atomic_write_text(dest: Path, text: str) -> None:
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".tmp")
    with open(tmp, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, dest)


def process_one(img_path: Path, overwrite: bool) -> Tuple[str, Path]:
    # classify
    if not img_path.exists():
        return "missing", img_path
    try:
        size = img_path.stat().st_size
    except Exception:
        return "missing", img_path

    if size == 0:
        return "empty", img_path

    xyz = parse_xyz(img_path)
    if xyz is None:
        return "bad_path", img_path

    z, x, y = xyz
    pgw_path = img_path.with_suffix(".pgw")

    if (not overwrite) and pgw_path.exists() and pgw_path.stat().st_size > 0:
        return "skip", img_path

    lines = compute_pgw_lines(z, x, y)
    atomic_write_text(pgw_path, lines)
    return "ok", img_path


def iter_images(root: Path, ext: str) -> list[Path]:
    # ext like ".png" or ".jpg"
    ext = ext.lower()
    return [p for p in root.rglob(f"*{ext}") if p.is_file()]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", required=True, help="Root folder containing tiles")
    ap.add_argument("--ext", default=".png", help="Image extension: .png or .jpg")
    ap.add_argument("--workers", type=int, default=16)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--log-dir", default="_PGW_LOGS", help="Log dir under root")
    ap.add_argument("--progress-every", type=int, default=20000)
    args = ap.parse_args()

    root = Path(args.root)
    log_dir = root / args.log_dir
    log_dir.mkdir(parents=True, exist_ok=True)

    missing_log = log_dir / "missing.log"
    empty_log = log_dir / "empty.log"
    bad_path_log = log_dir / "bad_path.log"
    fail_log = log_dir / "fail.log"

    imgs = iter_images(root, args.ext)
    total = len(imgs)

    print(f"Root   : {root}")
    print(f"Ext    : {args.ext}")
    print(f"Found  : {total:,} images")
    print(f"Workers: {args.workers}")
    print(f"Overwrite PGW: {args.overwrite}")
    print(f"Logs   : {log_dir}")

    ok = skip = missing = empty = bad_path = fail = 0

    # open logs once (line-buffered)
    with open(missing_log, "a", encoding="utf-8") as f_missing, \
         open(empty_log, "a", encoding="utf-8") as f_empty, \
         open(bad_path_log, "a", encoding="utf-8") as f_bad, \
         open(fail_log, "a", encoding="utf-8") as f_fail:

        with cf.ThreadPoolExecutor(max_workers=args.workers) as ex:
            futs = [ex.submit(process_one, p, args.overwrite) for p in imgs]

            for i, fut in enumerate(cf.as_completed(futs), 1):
                try:
                    status, p = fut.result()
                except Exception as e:
                    fail += 1
                    f_fail.write(f"[fail(exception:{repr(e)})] {p if 'p' in locals() else ''}\n")
                    continue

                if status == "ok":
                    ok += 1
                elif status == "skip":
                    skip += 1
                elif status == "missing":
                    missing += 1
                    f_missing.write(str(p) + "\n")
                elif status == "empty":
                    empty += 1
                    f_empty.write(str(p) + "\n")
                elif status == "bad_path":
                    bad_path += 1
                    f_bad.write(str(p) + "\n")
                else:
                    fail += 1
                    f_fail.write(f"[fail({status})] {p}\n")

                if i % args.progress_every == 0:
                    print(f"[PROGRESS] {i:,}/{total:,} ok={ok:,} skip={skip:,} empty={empty:,} missing={missing:,} bad_path={bad_path:,} fail={fail:,}")

    print("\n=== DONE ===")
    print(f"ok={ok:,} skip={skip:,} empty={empty:,} missing={missing:,} bad_path={bad_path:,} fail={fail:,}")


if __name__ == "__main__":
    main()