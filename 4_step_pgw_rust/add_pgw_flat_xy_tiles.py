#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256


def main() -> int:
    ap = argparse.ArgumentParser(description="Add missing PGW files for flat x_y.png tile folders by zoom.")
    ap.add_argument("--root", required=True, help="Root containing numeric zoom folders (e.g., 13..18).")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    created = 0
    skipped = 0
    bad = 0

    zoom_dirs = sorted([p for p in root.iterdir() if p.is_dir() and p.name.isdigit()], key=lambda p: int(p.name))
    for zdir in zoom_dirs:
        z = int(zdir.name)
        n = 1 << z
        res = WEBM_WORLD / (n * TILE_SIZE)
        for png in zdir.glob("*.png"):
            parts = png.stem.split("_")
            if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
                bad += 1
                continue
            x = int(parts[0])
            y = int(parts[1])
            pgw = png.with_suffix(".pgw")
            if pgw.exists() and pgw.stat().st_size > 0:
                skipped += 1
                continue

            minx = -WEBM_HALF + x * TILE_SIZE * res
            maxy = WEBM_HALF - y * TILE_SIZE * res
            c = minx + res / 2.0
            f = maxy - res / 2.0
            pgw.write_text(
                f"{res:.12f}\n0.000000000000\n0.000000000000\n{-res:.12f}\n{c:.12f}\n{f:.12f}\n",
                encoding="utf-8",
            )
            created += 1

    print(f"PGW done: created={created} skipped={skipped} bad_name={bad}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
