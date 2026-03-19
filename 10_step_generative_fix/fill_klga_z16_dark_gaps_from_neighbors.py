#!/usr/bin/env python3
from __future__ import annotations

import csv
import re
from pathlib import Path
from typing import Iterable

from PIL import Image, ImageStat

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256
ZOOM = 16


def parse_xy_from_name(name: str) -> tuple[int, int] | None:
    m = re.match(r"^(\d+)_(\d+)\.png$", name)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2))


def parse_report_paths(report: Path) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    if not report.exists():
        return out
    for line in report.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line:
            continue
        rel = line.split("\t")[0]
        xy = parse_xy_from_name(Path(rel).name)
        if xy:
            out.add(xy)
    return out


def parse_size_list(report: Path, max_bytes: int = 25000) -> set[tuple[int, int]]:
    out: set[tuple[int, int]] = set()
    if not report.exists():
        return out
    for line in report.read_text(encoding="utf-8", errors="replace").splitlines():
        parts = [p.strip() for p in line.split("\t") if p.strip()]
        if len(parts) < 2:
            continue
        xy = parse_xy_from_name(Path(parts[0]).name)
        if not xy:
            continue
        try:
            size = int(parts[1])
        except ValueError:
            continue
        if size <= max_bytes:
            out.add(xy)
    return out


def image_stats(img: Image.Image) -> tuple[float, float]:
    g = img.convert("L")
    st = ImageStat.Stat(g)
    return st.mean[0], st.stddev[0]


def tile_is_bad(tile_path: Path, tiny_bytes: int = 3500) -> tuple[bool, float, float, int]:
    if not tile_path.exists():
        return True, 0.0, 0.0, 0
    size = tile_path.stat().st_size
    try:
        with Image.open(tile_path) as im:
            rgb = im.convert("RGB")
            mean, std = image_stats(rgb)
    except Exception:
        return True, 0.0, 0.0, size
    if size <= tiny_bytes:
        return True, mean, std, size
    if mean < 5.0 and std < 3.0:
        return True, mean, std, size
    if std < 0.9 and mean < 75.0:
        return True, mean, std, size
    return False, mean, std, size


def ensure_worldfiles(png_path: Path, zoom: int = ZOOM) -> None:
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


def iter_neighbors(x: int, y: int, radius: int) -> Iterable[tuple[int, int]]:
    for r in range(1, radius + 1):
        for yy in range(y - r, y + r + 1):
            for xx in range(x - r, x + r + 1):
                if xx == x and yy == y:
                    continue
                if max(abs(xx - x), abs(yy - y)) != r:
                    continue
                yield xx, yy


def choose_best_neighbor(
    z16_dir: Path,
    x: int,
    y: int,
    avoid: set[tuple[int, int]],
    prefer_mean: float | None,
    prefer_std: float | None,
    max_radius: int = 8,
) -> tuple[Image.Image, tuple[int, int], float, float, int] | None:
    best = None
    best_score = 1e9

    for nx, ny in iter_neighbors(x, y, max_radius):
        if (nx, ny) in avoid:
            continue
        p = z16_dir / f"{nx}_{ny}.png"
        bad, mean, std, size = tile_is_bad(p)
        if bad:
            continue
        try:
            with Image.open(p) as im:
                cand = im.convert("RGB")
        except Exception:
            continue
        dist = max(abs(nx - x), abs(ny - y))
        mean_delta = abs(mean - prefer_mean) if prefer_mean is not None else 0.0
        std_delta = abs(std - prefer_std) if prefer_std is not None else 0.0
        score = dist * 5.0 + mean_delta * 0.6 + std_delta * 0.4
        if score < best_score:
            best_score = score
            best = (cand.copy(), (nx, ny), mean, std, size)
    return best


def collect_targets(project_root: Path) -> list[tuple[int, int]]:
    reports = project_root / "CODE" / "reports"
    targets: set[tuple[int, int]] = set()

    targets |= parse_report_paths(reports / "KLGA_Z16_still_bad_after_brcj.txt")
    targets |= parse_size_list(reports / "KLGA_Z16_r1_c2_baseX604_baseY769_suspect_tiles.txt", max_bytes=25000)

    # Strong fallback: include tiny tiles inside the two problematic cones.
    x0, x1 = 19296, 19359
    y0, y1 = 24608, 24639
    z16 = project_root / "KLGA" / "16"
    for y in range(y0, y1 + 1):
        for x in range(x0, x1 + 1):
            p = z16 / f"{x}_{y}.png"
            bad, _, _, size = tile_is_bad(p, tiny_bytes=3000)
            if bad and size <= 3000:
                targets.add((x, y))
    return sorted(targets)


def main() -> int:
    project_root = Path(__file__).resolve().parents[1]
    z16 = project_root / "KLGA" / "16"
    log_csv = project_root / "CODE" / "reports" / "KLGA_Z16_neighbor_fill_log.csv"
    targets = collect_targets(project_root)
    target_set = set(targets)

    log_csv.parent.mkdir(parents=True, exist_ok=True)
    with log_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(
            [
                "target_tile",
                "old_size_bytes",
                "old_mean",
                "old_std",
                "source_tile",
                "source_size_bytes",
                "source_mean",
                "source_std",
                "new_size_bytes",
                "status",
            ]
        )

        replaced = 0
        skipped = 0
        for x, y in targets:
            target_path = z16 / f"{x}_{y}.png"
            _, old_mean, old_std, old_size = tile_is_bad(target_path, tiny_bytes=10**9)

            choice = choose_best_neighbor(
                z16_dir=z16,
                x=x,
                y=y,
                avoid=target_set,
                prefer_mean=old_mean,
                prefer_std=old_std,
                max_radius=10,
            )
            if choice is None:
                w.writerow(
                    [f"{x}_{y}.png", old_size, f"{old_mean:.3f}", f"{old_std:.3f}", "", "", "", "", "", "skipped_no_neighbor"]
                )
                skipped += 1
                continue

            img, (sx, sy), src_mean, src_std, src_size = choice
            img.save(target_path, format="PNG")
            ensure_worldfiles(target_path, zoom=ZOOM)
            new_size = target_path.stat().st_size
            w.writerow(
                [
                    f"{x}_{y}.png",
                    old_size,
                    f"{old_mean:.3f}",
                    f"{old_std:.3f}",
                    f"{sx}_{sy}.png",
                    src_size,
                    f"{src_mean:.3f}",
                    f"{src_std:.3f}",
                    new_size,
                    "replaced_from_neighbor",
                ]
            )
            replaced += 1

    print(f"targets={len(targets)} replaced={replaced} skipped={skipped}")
    print(f"log={log_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
