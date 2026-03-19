#!/usr/bin/env python3
from __future__ import annotations

import argparse
import shutil
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser(description="Convert flat x_y.png tiles into nested z/x/y.png layout")
    ap.add_argument("--src", required=True)
    ap.add_argument("--dst", required=True)
    ap.add_argument("--ext", default=".png")
    args = ap.parse_args()

    src = Path(args.src)
    dst = Path(args.dst)
    ext = args.ext.lower()

    if not src.exists():
        raise SystemExit(f"Missing source: {src}")

    dst.mkdir(parents=True, exist_ok=True)

    copied = 0
    skipped = 0
    bad = 0

    for p in src.glob(f"*{ext}"):
        stem = p.stem
        parts = stem.split("_")
        if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
            bad += 1
            continue
        x, y = parts
        out_dir = dst / x
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{y}{ext}"
        if out_file.exists() and out_file.stat().st_size > 0:
            skipped += 1
            continue
        shutil.copy2(p, out_file)
        copied += 1

    print(f"Converted {src} -> {dst}: copied={copied} skipped={skipped} bad={bad}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
