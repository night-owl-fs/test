from __future__ import annotations

import csv
from pathlib import Path
from typing import Iterable, Tuple

from PIL import Image, ImageStat

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256


def parse_targets(report_file: Path) -> list[Tuple[int, int]]:
    targets: list[Tuple[int, int]] = []
    for line in report_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        parts = line.split("\t")
        if len(parts) < 2:
            continue
        rel = parts[1]
        name = Path(rel).name
        stem = Path(name).stem
        x_str, y_str = stem.split("_")
        targets.append((int(x_str), int(y_str)))
    return targets


def ensure_pgw(png_path: Path, zoom: int) -> None:
    x_str, y_str = png_path.stem.split("_")
    x, y = int(x_str), int(y_str)
    n = 1 << zoom
    res = WEBM_WORLD / (n * TILE_SIZE)
    minx = -WEBM_HALF + x * TILE_SIZE * res
    maxy = WEBM_HALF - y * TILE_SIZE * res
    c = minx + res / 2.0
    f = maxy - res / 2.0
    pgw = png_path.with_suffix(png_path.suffix + ".pgw")
    pgw.write_text(
        f"{res:.12f}\n0.000000000000\n0.000000000000\n{-res:.12f}\n{c:.12f}\n{f:.12f}\n",
        encoding="utf-8",
    )


def synth_from_z15(z15_dir: Path, x16: int, y16: int) -> Image.Image | None:
    x15 = x16 // 2
    y15 = y16 // 2
    src = z15_dir / f"{x15}_{y15}.png"
    if not src.exists():
        return None
    with Image.open(src) as im:
        im = im.convert("RGB")
        qx = x16 % 2
        qy = y16 % 2
        box = (qx * 128, qy * 128, qx * 128 + 128, qy * 128 + 128)
        part = im.crop(box)
        return part.resize((256, 256), Image.Resampling.BICUBIC)


def synth_from_z17(z17_dir: Path, x16: int, y16: int) -> Image.Image | None:
    x17 = x16 * 2
    y17 = y16 * 2
    mosaic = Image.new("RGB", (512, 512))
    found = 0
    for dy in (0, 1):
        for dx in (0, 1):
            src = z17_dir / f"{x17 + dx}_{y17 + dy}.png"
            if not src.exists():
                continue
            with Image.open(src) as im:
                im = im.convert("RGB")
                if im.size != (256, 256):
                    im = im.resize((256, 256), Image.Resampling.BILINEAR)
                mosaic.paste(im, (dx * 256, dy * 256))
                found += 1
    if found == 0:
        return None
    if found < 4:
        # fill missing quadrants with nearest present block to avoid hard black squares
        for dy in (0, 1):
            for dx in (0, 1):
                quad = mosaic.crop((dx * 256, dy * 256, dx * 256 + 256, dy * 256 + 256))
                if quad.getbbox() is None:
                    for ddy in (0, 1):
                        for ddx in (0, 1):
                            cand = mosaic.crop((ddx * 256, ddy * 256, ddx * 256 + 256, ddy * 256 + 256))
                            if cand.getbbox() is not None:
                                mosaic.paste(cand, (dx * 256, dy * 256))
                                break
                        else:
                            continue
                        break
    return mosaic.resize((256, 256), Image.Resampling.LANCZOS)


def image_quality(img: Image.Image) -> Tuple[float, float]:
    g = img.convert("L")
    st = ImageStat.Stat(g)
    return st.mean[0], st.stddev[0]


def is_flat_dark(img: Image.Image) -> bool:
    mean, std = image_quality(img)
    if mean < 3.0 and std < 3.0:
        return True
    if std < 1.0 and mean < 60.0:
        return True
    return False


def nearest_good_neighbor(
    z16_dir: Path,
    x: int,
    y: int,
    avoid: set[Tuple[int, int]],
    max_radius: int = 5,
) -> Image.Image | None:
    best_img: Image.Image | None = None
    best_score = -1.0

    for r in range(1, max_radius + 1):
        for yy in range(y - r, y + r + 1):
            for xx in range(x - r, x + r + 1):
                if xx == x and yy == y:
                    continue
                if (xx, yy) in avoid:
                    continue
                p = z16_dir / f"{xx}_{yy}.png"
                if not p.exists():
                    continue
                try:
                    with Image.open(p) as im:
                        cand = im.convert("RGB")
                except Exception:
                    continue

                mean, std = image_quality(cand)
                if mean < 3.0 and std < 3.0:
                    continue
                score = std
                if score > best_score:
                    best_score = score
                    best_img = cand
        if best_img is not None:
            return best_img
    return best_img


def patch_tiles(root: Path, report_file: Path, out_csv: Path) -> None:
    z15 = root / "KLGA" / "15"
    z16 = root / "KLGA" / "16"
    z17 = root / "KLGA" / "17"
    targets = parse_targets(report_file)
    target_set = set(targets)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["tile", "used_z15", "used_z17", "status", "new_size_bytes"])

        for x, y in targets:
            tile_name = f"{x}_{y}.png"
            z16_path = z16 / tile_name
            src15 = synth_from_z15(z15, x, y)
            src17 = synth_from_z17(z17, x, y)

            used15 = src15 is not None
            used17 = src17 is not None

            if src15 is None and src17 is None:
                w.writerow([tile_name, used15, used17, "skipped_no_sources", ""])
                continue

            if src15 is not None and src17 is not None:
                out = Image.blend(src15, src17, alpha=0.5)
            else:
                out = src15 if src15 is not None else src17

            assert out is not None
            mode = "blend_adjacent_zooms"
            if is_flat_dark(out):
                fallback = nearest_good_neighbor(z16, x, y, avoid=target_set)
                if fallback is not None:
                    out = fallback
                    mode = "neighbor_fallback"

            out.save(z16_path, format="PNG")
            ensure_pgw(z16_path, zoom=16)
            new_size = z16_path.stat().st_size
            w.writerow([tile_name, used15, used17, mode, new_size])


if __name__ == "__main__":
    project_root = Path(__file__).resolve().parents[1]
    report = project_root / "CODE" / "reports" / "KLGA_Z16_visual_poison_tiles.txt"
    out_csv = project_root / "CODE" / "reports" / "KLGA_Z16_patch_run.csv"
    patch_tiles(project_root, report, out_csv)
    print(f"Patched tiles listed in: {out_csv}")
