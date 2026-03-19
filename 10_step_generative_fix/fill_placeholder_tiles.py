#!/usr/bin/env python3
"""
Fill placeholder imagery tiles using nearby real imagery.

Replacement priority:
1) Downsample from four higher-zoom child tiles.
2) Upsample the matching quadrant from the lower-zoom parent tile.
3) Blend nearest non-placeholder neighbors at the same zoom.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Sequence, Tuple

from PIL import Image

TILE_SIZE = 256
HALF_TILE = TILE_SIZE // 2
DEFAULT_FILLER_HASHES = [
    # ArcGIS "Map data not yet available" placeholder from this dataset.
    "f27d9de7f80c13501f470595e327aa6d",
]


@dataclass
class TileRef:
    airport: str
    z: int
    x: int
    y: int

    @property
    def rel(self) -> str:
        return f"{self.airport}/{self.z}/{self.x}/{self.y}.png"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replace placeholder PNG tiles in-place.")
    parser.add_argument("--root", required=True, help="Tile root containing AIRPORT/z/x/y.png layout")
    parser.add_argument("--airport", required=True, help="Airport folder (ex: KTPA)")
    parser.add_argument(
        "--zooms",
        default="14,15",
        help="Comma list of zooms to repair (default: 14,15)",
    )
    parser.add_argument(
        "--filler-hash",
        action="append",
        default=[],
        help="Placeholder tile MD5 hash. Can be passed multiple times.",
    )
    parser.add_argument(
        "--max-neighbor-radius",
        type=int,
        default=8,
        help="Max Manhattan search radius for neighbor fallback",
    )
    parser.add_argument(
        "--report",
        default="fill_placeholder_report.json",
        help="Output JSON report path",
    )
    return parser.parse_args()


def tile_path(root: Path, ref: TileRef) -> Path:
    return root / ref.airport / str(ref.z) / str(ref.x) / f"{ref.y}.png"


def file_md5(path: Path) -> str:
    h = hashlib.md5()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 16), b""):
            h.update(chunk)
    return h.hexdigest()


def parse_zoom_list(raw: str) -> List[int]:
    out = []
    for part in raw.split(","):
        p = part.strip()
        if not p:
            continue
        out.append(int(p))
    return sorted(set(out))


class TileRepair:
    def __init__(self, root: Path, airport: str, filler_hashes: Sequence[str], max_neighbor_radius: int):
        self.root = root
        self.airport = airport
        self.filler_hashes = set(h.lower() for h in filler_hashes)
        self.max_neighbor_radius = max(1, max_neighbor_radius)
        self.hash_cache: Dict[Path, str] = {}

    def hash_of(self, path: Path) -> Optional[str]:
        if not path.exists():
            return None
        cached = self.hash_cache.get(path)
        if cached is not None:
            return cached
        h = file_md5(path)
        self.hash_cache[path] = h
        return h

    def is_filler(self, path: Path) -> bool:
        h = self.hash_of(path)
        return h in self.filler_hashes if h else False

    @staticmethod
    def load_rgb(path: Path) -> Image.Image:
        with Image.open(path) as im:
            return im.convert("RGB")

    def gather_targets(self, zooms: Sequence[int]) -> List[TileRef]:
        targets: List[TileRef] = []
        base = self.root / self.airport
        for z in zooms:
            zdir = base / str(z)
            if not zdir.exists():
                continue
            for x_entry in zdir.iterdir():
                if not x_entry.is_dir():
                    continue
                try:
                    x = int(x_entry.name)
                except ValueError:
                    continue
                for y_file in x_entry.iterdir():
                    if not y_file.is_file() or y_file.suffix.lower() != ".png":
                        continue
                    try:
                        y = int(y_file.stem)
                    except ValueError:
                        continue
                    if self.is_filler(y_file):
                        targets.append(TileRef(self.airport, z, x, y))
        targets.sort(key=lambda t: (t.z, t.x, t.y))
        return targets

    def _child_downsample(self, ref: TileRef) -> Optional[Image.Image]:
        child_z = ref.z + 1
        children: List[Image.Image] = []
        for dy in (0, 1):
            for dx in (0, 1):
                child = TileRef(ref.airport, child_z, ref.x * 2 + dx, ref.y * 2 + dy)
                cpath = tile_path(self.root, child)
                if not cpath.exists() or self.is_filler(cpath):
                    return None
                children.append(self.load_rgb(cpath))
        mosaic = Image.new("RGB", (TILE_SIZE * 2, TILE_SIZE * 2))
        mosaic.paste(children[0], (0, 0))
        mosaic.paste(children[1], (TILE_SIZE, 0))
        mosaic.paste(children[2], (0, TILE_SIZE))
        mosaic.paste(children[3], (TILE_SIZE, TILE_SIZE))
        return mosaic.resize((TILE_SIZE, TILE_SIZE), Image.Resampling.LANCZOS)

    def _parent_upsample(self, ref: TileRef) -> Optional[Image.Image]:
        if ref.z <= 0:
            return None
        parent = TileRef(ref.airport, ref.z - 1, ref.x // 2, ref.y // 2)
        ppath = tile_path(self.root, parent)
        if not ppath.exists() or self.is_filler(ppath):
            return None
        pimg = self.load_rgb(ppath)
        qx = HALF_TILE if (ref.x % 2) else 0
        qy = HALF_TILE if (ref.y % 2) else 0
        crop = pimg.crop((qx, qy, qx + HALF_TILE, qy + HALF_TILE))
        return crop.resize((TILE_SIZE, TILE_SIZE), Image.Resampling.LANCZOS)

    def _neighbor_blend(self, ref: TileRef) -> Optional[Image.Image]:
        candidates: List[Tuple[int, Image.Image]] = []
        for radius in range(1, self.max_neighbor_radius + 1):
            # Scan the ring around (x,y) for this radius.
            for dx in range(-radius, radius + 1):
                for dy in range(-radius, radius + 1):
                    if dx == 0 and dy == 0:
                        continue
                    if abs(dx) + abs(dy) != radius:
                        continue
                    nref = TileRef(ref.airport, ref.z, ref.x + dx, ref.y + dy)
                    np = tile_path(self.root, nref)
                    if not np.exists() or self.is_filler(np):
                        continue
                    candidates.append((radius, self.load_rgb(np)))
            if len(candidates) >= 4:
                break
        if not candidates:
            return None
        imgs = [im for _, im in candidates[:4]]
        blended = imgs[0]
        for idx, im in enumerate(imgs[1:], start=2):
            blended = Image.blend(blended, im, 1.0 / idx)
        return blended

    def build_replacement(self, ref: TileRef) -> Tuple[Optional[Image.Image], str]:
        img = self._child_downsample(ref)
        if img is not None:
            return img, "child_downsample"
        img = self._parent_upsample(ref)
        if img is not None:
            return img, "parent_upsample"
        img = self._neighbor_blend(ref)
        if img is not None:
            return img, "neighbor_blend"
        return None, "unresolved"

    def save_replacement(self, ref: TileRef, img: Image.Image) -> None:
        out = tile_path(self.root, ref)
        out.parent.mkdir(parents=True, exist_ok=True)
        img.save(out, format="PNG", optimize=True)
        # Refresh cache entry after write.
        self.hash_cache[out] = file_md5(out)


def main() -> None:
    args = parse_args()
    root = Path(args.root).resolve()
    zooms = parse_zoom_list(args.zooms)
    filler_hashes = list(DEFAULT_FILLER_HASHES)
    filler_hashes.extend(args.filler_hash)

    repair = TileRepair(
        root=root,
        airport=args.airport.upper(),
        filler_hashes=filler_hashes,
        max_neighbor_radius=args.max_neighbor_radius,
    )
    targets = repair.gather_targets(zooms)
    before = len(targets)

    method_counts: Counter[str] = Counter()
    unresolved: List[str] = []
    for ref in targets:
        replacement, method = repair.build_replacement(ref)
        method_counts[method] += 1
        if replacement is None:
            unresolved.append(ref.rel)
            continue
        repair.save_replacement(ref, replacement)

    # Re-check remaining fillers on requested zooms.
    remaining = repair.gather_targets(zooms)

    report = {
        "root": str(root),
        "airport": repair.airport,
        "zooms": zooms,
        "filler_hashes": sorted(repair.filler_hashes),
        "before_filler_count": before,
        "after_filler_count": len(remaining),
        "replaced_count": before - len(remaining),
        "method_counts": dict(method_counts),
        "remaining_samples": [t.rel for t in remaining[:50]],
        "unresolved_samples": unresolved[:50],
    }

    report_path = Path(args.report)
    if not report_path.is_absolute():
        report_path = Path.cwd() / report_path
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2))

    print(json.dumps(report, indent=2))
    print(f"report={report_path}")


if __name__ == "__main__":
    main()
