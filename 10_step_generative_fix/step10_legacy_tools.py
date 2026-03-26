#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import math
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
TILE_SIZE = 256
AIRPORT_FOLDER_ALIASES = {"KCRQ": "KRCQ"}


def write_worldfiles_png(png_path: Path, zoom: int) -> None:
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


def parse_xy_name(path: Path) -> tuple[int, int] | None:
    parts = path.stem.split("_")
    if len(parts) != 2 or not parts[0].isdigit() or not parts[1].isdigit():
        return None
    return int(parts[0]), int(parts[1])


def build_url(template: str, airport: str, z: int, x: int, y: int, ext: str) -> str:
    rel_path = f"{airport}/{z}/{x}_{y}.{ext}"
    return template.format(
        airport=airport,
        z=z,
        x=x,
        y=y,
        ext=ext,
        path=rel_path,
    )


def valid_download(path: Path, min_bytes: int) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size >= min_bytes


def download_file(
    url: str,
    dst: Path,
    retries: int,
    timeout: int,
    min_bytes: int,
) -> tuple[bool, str]:
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")
    last_error = ""

    for attempt in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "step10-legacy-tools/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as response, tmp.open("wb") as handle:
                handle.write(response.read())
            tmp.replace(dst)
            if valid_download(dst, min_bytes):
                return True, ""
            dst.unlink(missing_ok=True)
            last_error = f"too small (<{min_bytes} bytes)"
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as exc:
            last_error = str(exc)
        tmp.unlink(missing_ok=True)
        if attempt < retries:
            time.sleep(1.5)

    return False, last_error


def parse_window_spec(raw: str) -> tuple[str, int, int, int, int, int]:
    parts = raw.strip().split(":")
    if len(parts) != 4:
        raise ValueError(
            f"bad window spec '{raw}' (expected AIRPORT:ZOOM:XMIN-XMAX:YMIN-YMAX)"
        )
    airport = parts[0].strip().upper()
    zoom = int(parts[1].strip())
    x_range = parts[2].split("-", 1)
    y_range = parts[3].split("-", 1)
    if len(x_range) != 2 or len(y_range) != 2:
        raise ValueError(
            f"bad window spec '{raw}' (expected AIRPORT:ZOOM:XMIN-XMAX:YMIN-YMAX)"
        )
    x_min, x_max = int(x_range[0]), int(x_range[1])
    y_min, y_max = int(y_range[0]), int(y_range[1])
    if x_min > x_max or y_min > y_max:
        raise ValueError(f"bad window spec '{raw}' (min must be <= max)")
    return airport, zoom, x_min, x_max, y_min, y_max


def cmd_download_window(args: argparse.Namespace) -> int:
    jobs: list[tuple[str, Path, int]] = []
    dest_root = Path(args.dest_root)

    for raw in args.window:
        airport, zoom, x_min, x_max, y_min, y_max = parse_window_spec(raw)
        for x in range(x_min, x_max + 1):
            for y in range(y_min, y_max + 1):
                png_out = dest_root / airport / str(zoom) / f"{x}_{y}.png"
                png_url = build_url(args.url_template, airport, zoom, x, y, "png")
                jobs.append((png_url, png_out, 100))
                if args.include_pgw:
                    pgw_out = dest_root / airport / str(zoom) / f"{x}_{y}.pgw"
                    pgw_url = build_url(args.url_template, airport, zoom, x, y, "pgw")
                    jobs.append((pgw_url, pgw_out, 20))

    print(f"planned_downloads={len(jobs)}")
    if args.dry_run:
        for url, dst, _ in jobs[:40]:
            print(f"DRY {url} -> {dst}")
        if len(jobs) > 40:
            print(f"... and {len(jobs) - 40} more")
        return 0

    failures: list[str] = []
    ok = 0

    def worker(job: tuple[str, Path, int]) -> tuple[bool, str]:
        url, dst, min_bytes = job
        if (not args.overwrite_existing) and valid_download(dst, min_bytes):
            return True, ""
        good, message = download_file(url, dst, args.retries, args.timeout, min_bytes)
        if good:
            return True, ""
        return False, f"{url} -> {dst}: {message}"

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(worker, job) for job in jobs]
        for index, future in enumerate(as_completed(futures), 1):
            good, message = future.result()
            if good:
                ok += 1
            else:
                failures.append(message)
            if index % 250 == 0 or index == len(futures):
                print(f"progress={index}/{len(futures)} ok={ok} fail={len(failures)}")

    if failures and args.failure_report:
        report = Path(args.failure_report)
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text("\n".join(failures) + "\n", encoding="utf-8")
        print(f"failure_report={report}")

    print(f"downloaded_ok={ok}")
    print(f"downloaded_fail={len(failures)}")
    return 0 if not failures else 2


def cmd_flatten_xy(args: argparse.Namespace) -> int:
    src = Path(args.src)
    dst = Path(args.dst)
    ext = args.ext.lower()

    if not src.exists():
        raise SystemExit(f"missing source: {src}")

    dst.mkdir(parents=True, exist_ok=True)
    copied = 0
    skipped = 0
    bad = 0

    for tile in src.glob(f"*{ext}"):
        xy = parse_xy_name(tile)
        if xy is None:
            bad += 1
            continue
        x, y = xy
        out_dir = dst / str(x)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_file = out_dir / f"{y}{ext}"
        if out_file.exists() and out_file.stat().st_size > 0:
            skipped += 1
            continue
        shutil.copy2(tile, out_file)
        copied += 1

    print(f"copied={copied}")
    print(f"skipped={skipped}")
    print(f"bad_names={bad}")
    return 0


def cmd_paint_window(args: argparse.Namespace) -> int:
    from PIL import Image

    src = Path(args.image).resolve()
    tile_dir = Path(args.tile_dir).resolve()
    out_px = args.tiles * TILE_SIZE

    with Image.open(src) as image:
        fill = image.convert("RGB").resize((out_px, out_px), Image.Resampling.LANCZOS)

    replaced = 0
    for row in range(args.tiles):
        for col in range(args.tiles):
            x = args.x0 + col
            y = args.y0 + row
            crop = fill.crop(
                (
                    col * TILE_SIZE,
                    row * TILE_SIZE,
                    (col + 1) * TILE_SIZE,
                    (row + 1) * TILE_SIZE,
                )
            )
            out_png = tile_dir / f"{x}_{y}.png"
            out_png.parent.mkdir(parents=True, exist_ok=True)
            crop.save(out_png, format="PNG")
            if not args.no_worldfiles:
                write_worldfiles_png(out_png, args.zoom)
            replaced += 1

    print(f"replaced_tiles={replaced}")
    print(f"image={src}")
    print(f"window=x:{args.x0}-{args.x0 + args.tiles - 1}, y:{args.y0}-{args.y0 + args.tiles - 1}")
    return 0


def run_command(cmd: list[str]) -> tuple[bool, str]:
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return result.returncode == 0, result.stdout


def to_win_path(path: Path | str) -> str:
    raw = str(path)
    if raw.startswith("/"):
        result = subprocess.run(
            ["wslpath", "-w", raw],
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    return raw


def resolve_gdal(name: str) -> str:
    for base in (Path("/mnt/c/OSGeo4W/bin"), Path(r"C:\OSGeo4W\bin")):
        candidate = base / name
        if candidate.exists():
            return str(candidate)
    return name


def valid_png(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 100


def collect_airport_dirs(root: Path) -> list[Path]:
    return sorted(
        [
            path
            for path in root.iterdir()
            if path.is_dir() and path.name.startswith("K") and len(path.name) == 4
        ]
    )


def best_source_tile(
    preferred_airport_dir: Path,
    z: int,
    x: int,
    y: int,
    airport_dirs: list[Path],
) -> Path | None:
    rel = Path(str(z)) / f"{x}_{y}.png"
    candidates: list[Path] = []

    primary = preferred_airport_dir / rel
    if valid_png(primary):
        candidates.append(primary)

    for airport_dir in airport_dirs:
        if airport_dir == preferred_airport_dir:
            continue
        candidate = airport_dir / rel
        if valid_png(candidate):
            candidates.append(candidate)

    if not candidates:
        return None
    return max(candidates, key=lambda tile: tile.stat().st_size)


def quality_score_gdal(tif_path: Path) -> float:
    gdalinfo = resolve_gdal("gdalinfo.exe")
    ok, output = run_command([gdalinfo, "-json", "-stats", to_win_path(tif_path)])
    if not ok:
        return -1.0
    try:
        payload = json.loads(output)
        bands = payload.get("bands", [])
        if not bands:
            return -1.0
        score = 0.0
        for band in bands[:3]:
            metadata = (band.get("metadata") or {}).get("", {})
            minimum = float(metadata.get("STATISTICS_MINIMUM", "0"))
            maximum = float(metadata.get("STATISTICS_MAXIMUM", "0"))
            mean = float(metadata.get("STATISTICS_MEAN", "0"))
            stddev = float(metadata.get("STATISTICS_STDDEV", "0"))
            score += (stddev * 4.0) + ((maximum - minimum) * 0.2) + (mean * 0.05)
        score += min(tif_path.stat().st_size / 1_000_000.0, 200.0)
        return score
    except Exception:
        return -1.0


def parse_target_metadata(tif_path: Path) -> dict[str, int]:
    name = tif_path.name
    prefix, suffix = name.split("_fromZ", 1)
    out_zoom = int(prefix[1:])
    base_part, remainder = suffix.split("_r", 1)
    base_zoom = int(base_part)
    row_part, remainder = remainder.split("_c", 1)
    col_part, remainder = remainder.split("_baseX", 1)
    base_x_part, base_y_part = remainder.split("_baseY", 1)
    base_y_part = base_y_part.removesuffix(".tif")
    return {
        "out_z": out_zoom,
        "base_z": base_zoom,
        "row": int(row_part),
        "col": int(col_part),
        "bx": int(base_x_part),
        "by": int(base_y_part),
    }


def cone_bounds(out_z: int, base_z: int, bx: int, by: int) -> tuple[float, float, float, float, int]:
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


def source_tile_range(
    minx: float,
    miny: float,
    maxx: float,
    maxy: float,
    src_z: int,
) -> tuple[int, int, int, int]:
    n = 1 << src_z
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


def build_from_zoom(
    airport_dir: Path,
    airport_dirs: list[Path],
    tif_path: Path,
    out_z: int,
    base_z: int,
    bx: int,
    by: int,
    src_z: int,
) -> tuple[bool, str]:
    minx, miny, maxx, maxy, factor = cone_bounds(out_z, base_z, bx, by)
    x_min, x_max, y_min, y_max = source_tile_range(minx, miny, maxx, maxy, src_z)
    src_dir = airport_dir / str(src_z)
    if not src_dir.exists():
        return False, f"missing zoom dir {src_dir}"

    source_tiles: list[Path] = []
    for y in range(y_min, y_max + 1):
        for x in range(x_min, x_max + 1):
            tile = best_source_tile(airport_dir, src_z, x, y, airport_dirs)
            if tile is None:
                return False, f"missing/corrupt source tile {x}_{y}.png at Z{src_z}"
            source_tiles.append(tile)

    gdalbuildvrt = resolve_gdal("gdalbuildvrt.exe")
    gdal_translate = resolve_gdal("gdal_translate.exe")
    out_px = factor * 256
    out_vrt = tif_path.with_suffix(".vrt")

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as handle:
        list_path = Path(handle.name)
        for tile in source_tiles:
            handle.write(to_win_path(tile) + "\n")

    ok, message = run_command(
        [gdalbuildvrt, "-q", "-input_file_list", to_win_path(list_path), to_win_path(out_vrt)]
    )
    list_path.unlink(missing_ok=True)
    if not ok:
        return False, message

    ok, message = run_command(
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
            str(out_px),
            str(out_px),
            to_win_path(out_vrt),
            to_win_path(tif_path),
        ]
    )
    if not ok:
        tif_path.unlink(missing_ok=True)
        return False, message

    return True, f"from Z{src_z}"


def cmd_rebuild_cones(args: argparse.Namespace) -> int:
    root = Path(args.root).resolve()
    targets = [Path(item) for item in (args.file or [])]
    if args.file_list:
        file_list = Path(args.file_list).resolve()
        for line in file_list.read_text(encoding="utf-8", errors="replace").splitlines():
            candidate = line.strip()
            if candidate:
                targets.append(Path(candidate))
    if not targets:
        raise SystemExit("no targets provided; use --file and/or --file-list")

    airport_dirs = collect_airport_dirs(root)
    rebuilt = 0
    failed = 0

    for target in targets:
        tif = (root / target).resolve() if not target.is_absolute() else target.resolve()
        meta = parse_target_metadata(tif)
        airport = tif.parent.parent.name
        airport_dir = root / airport
        if not airport_dir.exists() and airport in AIRPORT_FOLDER_ALIASES:
            airport_dir = root / AIRPORT_FOLDER_ALIASES[airport]

        out_z = meta["out_z"]
        candidates = [out_z + 1, out_z + 2, out_z - 1, out_z + 3, out_z - 2, out_z + 4, out_z - 3]
        candidates = [z for z in candidates if 13 <= z <= 18 and z != out_z]
        if args.force_src_zoom:
            candidates = [z for z in args.force_src_zoom if 13 <= z <= 18 and z != out_z]

        if args.best_size:
            best_path = None
            best_score = -1.0
            best_src = None
            for src_z in candidates:
                candidate_path = tif.with_name(f"{tif.stem}.src{src_z}.tif")
                ok, _ = build_from_zoom(
                    airport_dir=airport_dir,
                    airport_dirs=airport_dirs,
                    tif_path=candidate_path,
                    out_z=meta["out_z"],
                    base_z=meta["base_z"],
                    bx=meta["bx"],
                    by=meta["by"],
                    src_z=src_z,
                )
                if ok and candidate_path.exists():
                    score = quality_score_gdal(candidate_path)
                    if score > best_score:
                        best_score = score
                        best_path = candidate_path
                        best_src = src_z
                elif candidate_path.exists():
                    candidate_path.unlink(missing_ok=True)
                candidate_path.with_suffix(".vrt").unlink(missing_ok=True)

            if best_path is None:
                failed += 1
                print(f"[FAIL] {tif}")
            else:
                best_path.replace(tif)
                print(f"[OK] {tif} from Z{best_src} score={best_score:.2f}")
                rebuilt += 1
                for src_z in candidates:
                    candidate_path = tif.with_name(f"{tif.stem}.src{src_z}.tif")
                    candidate_path.unlink(missing_ok=True)
                    candidate_path.with_suffix(".vrt").unlink(missing_ok=True)
        else:
            ok_any = False
            for src_z in candidates:
                tmp = tif.with_name(f"{tif.stem}.tmp.tif")
                ok, message = build_from_zoom(
                    airport_dir=airport_dir,
                    airport_dirs=airport_dirs,
                    tif_path=tmp,
                    out_z=meta["out_z"],
                    base_z=meta["base_z"],
                    bx=meta["bx"],
                    by=meta["by"],
                    src_z=src_z,
                )
                if ok:
                    tmp.replace(tif)
                    print(f"[OK] {tif} {message}")
                    rebuilt += 1
                    ok_any = True
                    break
                tmp.unlink(missing_ok=True)
                tmp.with_suffix(".vrt").unlink(missing_ok=True)
            if not ok_any:
                failed += 1
                print(f"[FAIL] {tif}")

    print(f"rebuilt={rebuilt}")
    print(f"failed={failed}")
    return 0 if failed == 0 else 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Consolidated legacy maintenance utilities for step 10.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    download = subparsers.add_parser(
        "download-window",
        help="Download one or more airport tile windows from a URL template.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    download.add_argument(
        "--url-template",
        required=True,
        help="Supports {airport} {z} {x} {y} {ext} {path}.",
    )
    download.add_argument("--dest-root", default=".")
    download.add_argument(
        "--window",
        action="append",
        required=True,
        help="AIRPORT:ZOOM:XMIN-XMAX:YMIN-YMAX",
    )
    download.add_argument("--include-pgw", action="store_true")
    download.add_argument("--workers", type=int, default=20)
    download.add_argument("--retries", type=int, default=4)
    download.add_argument("--timeout", type=int, default=30)
    download.add_argument("--overwrite-existing", action="store_true")
    download.add_argument("--dry-run", action="store_true")
    download.add_argument("--failure-report")
    download.set_defaults(func=cmd_download_window)

    flatten = subparsers.add_parser(
        "flatten-xy",
        help="Convert flat x_y.png tiles into nested x/y.png layout.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    flatten.add_argument("--src", required=True)
    flatten.add_argument("--dst", required=True)
    flatten.add_argument("--ext", default=".png")
    flatten.set_defaults(func=cmd_flatten_xy)

    paint = subparsers.add_parser(
        "paint-window",
        help="Apply one fill image across a flat x_y.png tile window.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    paint.add_argument("--image", required=True)
    paint.add_argument("--tile-dir", required=True)
    paint.add_argument("--zoom", required=True, type=int)
    paint.add_argument("--x0", required=True, type=int)
    paint.add_argument("--y0", required=True, type=int)
    paint.add_argument("--tiles", default=32, type=int)
    paint.add_argument("--no-worldfiles", action="store_true")
    paint.set_defaults(func=cmd_paint_window)

    rebuild = subparsers.add_parser(
        "rebuild-cones",
        help="Rebuild target GeoTIFF cones from nearby zoom levels with GDAL.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    rebuild.add_argument("--root", default=".")
    rebuild.add_argument("--file", action="append")
    rebuild.add_argument("--file-list")
    rebuild.add_argument("--best-size", action="store_true")
    rebuild.add_argument("--force-src-zoom", action="append", type=int)
    rebuild.set_defaults(func=cmd_rebuild_cones)

    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
