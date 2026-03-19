#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from PIL import Image

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256


def write_worldfiles(png_path: Path, zoom: int) -> None:
    x_str, y_str = png_path.stem.split("_")
    x, y = int(x_str), int(y_str)
    n = 1 << zoom
    res = WEBM_WORLD / (n * TILE_SIZE)
    minx = -WEBM_HALF + x * TILE_SIZE * res
    maxy = WEBM_HALF - y * TILE_SIZE * res
    c = minx + res / 2.0
    f = maxy - res / 2.0
    txt = (
        f"{res:.12f}\n"
        "0.000000000000\n"
        "0.000000000000\n"
        f"{-res:.12f}\n"
        f"{c:.12f}\n"
        f"{f:.12f}\n"
    )
    png_path.with_suffix(".pgw").write_text(txt, encoding="utf-8")
    png_path.with_suffix(png_path.suffix + ".pgw").write_text(txt, encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description="Apply a fill image to an XY tile window.")
    ap.add_argument("--image", required=True, help="Source fill image path.")
    ap.add_argument("--tile-dir", required=True, help="Folder containing x_y.png tiles.")
    ap.add_argument("--zoom", type=int, required=True, help="Tile zoom level.")
    ap.add_argument("--x0", type=int, required=True, help="Top-left tile X.")
    ap.add_argument("--y0", type=int, required=True, help="Top-left tile Y.")
    ap.add_argument("--tiles", type=int, default=32, help="Tiles per side in fill window.")
    args = ap.parse_args()

    src = Path(args.image).resolve()
    tile_dir = Path(args.tile_dir).resolve()
    out_px = args.tiles * TILE_SIZE

    with Image.open(src) as im:
        fill = im.convert("RGB").resize((out_px, out_px), Image.Resampling.LANCZOS)

    replaced = 0
    for r in range(args.tiles):
        for c in range(args.tiles):
            x = args.x0 + c
            y = args.y0 + r
            crop = fill.crop((c * TILE_SIZE, r * TILE_SIZE, (c + 1) * TILE_SIZE, (r + 1) * TILE_SIZE))
            out_png = tile_dir / f"{x}_{y}.png"
            crop.save(out_png, format="PNG")
            write_worldfiles(out_png, args.zoom)
            replaced += 1

    print(f"replaced_tiles={replaced}")
    print(f"image={src}")
    print(f"window=x:{args.x0}-{args.x0 + args.tiles - 1}, y:{args.y0}-{args.y0 + args.tiles - 1}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
