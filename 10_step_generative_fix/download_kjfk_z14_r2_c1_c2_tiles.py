#!/usr/bin/env python3
"""Download only the source tiles needed for:
- Z14_fromZ9_r2_c1_baseX151_baseY193
- Z14_fromZ9_r2_c2_baseX152_baseY193

This corresponds to KJFK Z14 tile ranges:
- x: 4832..4895
- y: 6176..6207
Total PNG tiles: 2048
"""

from __future__ import annotations

import argparse
import pathlib
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def build_url(template: str, airport: str, z: int, x: int, y: int, ext: str) -> str:
    rel_path = f"{airport}/{z}/{x}_{y}.{ext}"
    return template.format(airport=airport, z=z, x=x, y=y, ext=ext, path=rel_path)


def valid(path: pathlib.Path, min_bytes: int) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size >= min_bytes


def download(url: str, dst: pathlib.Path, retries: int, timeout: int, min_bytes: int):
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp = dst.with_suffix(dst.suffix + ".part")

    last = ""
    for i in range(1, retries + 1):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "tile-fetch/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r, tmp.open("wb") as f:
                f.write(r.read())
            tmp.replace(dst)
            if valid(dst, min_bytes):
                return True, ""
            dst.unlink(missing_ok=True)
            last = f"too small (<{min_bytes})"
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            last = str(e)
        tmp.unlink(missing_ok=True)
        if i < retries:
            time.sleep(1.5)
    return False, last


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url-template", required=True, help="Supports {airport} {z} {x} {y} {ext} {path}")
    ap.add_argument("--dest-root", default=r"C:\Users\ravery\Downloads\AIRPORT_TILES")
    ap.add_argument("--airport", default="KJFK")
    ap.add_argument("--zoom", type=int, default=14)
    ap.add_argument("--include-pgw", action="store_true")
    ap.add_argument("--workers", type=int, default=20)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--overwrite-existing", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    x_min, x_max = 4832, 4895
    y_min, y_max = 6176, 6207

    dest_root = pathlib.Path(args.dest_root)
    jobs = []
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            png_rel = pathlib.Path(args.airport) / str(args.zoom) / f"{x}_{y}.png"
            png_out = dest_root / png_rel
            png_url = build_url(args.url_template, args.airport, args.zoom, x, y, "png")
            jobs.append((png_url, png_out, 100))

            if args.include_pgw:
                pgw_rel = pathlib.Path(args.airport) / str(args.zoom) / f"{x}_{y}.pgw"
                pgw_out = dest_root / pgw_rel
                pgw_url = build_url(args.url_template, args.airport, args.zoom, x, y, "pgw")
                jobs.append((pgw_url, pgw_out, 20))

    print(f"Planned downloads: {len(jobs)}")
    if args.dry_run:
        for u, p, _ in jobs[:30]:
            print(f"DRY {u} -> {p}")
        if len(jobs) > 30:
            print(f"... and {len(jobs) - 30} more")
        return

    ok = 0
    fail = []

    def worker(job):
        url, dst, min_b = job
        if (not args.overwrite_existing) and valid(dst, min_b):
            return True, ""
        return download(url, dst, args.retries, args.timeout, min_b)

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(worker, j) for j in jobs]
        for i, fut in enumerate(as_completed(futs), 1):
            good, err = fut.result()
            if good:
                ok += 1
            else:
                fail.append(err)
            if i % 250 == 0 or i == len(futs):
                print(f"Progress {i}/{len(futs)} ok={ok} fail={len(fail)}")

    if fail:
        out = pathlib.Path("CODE/reports/kjfk_z14_r2_c1_c2_download_failures.txt")
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text("\n".join(fail) + "\n", encoding="utf-8")
        print(f"Failures written to {out}")

    print(f"Done. OK={ok} FAIL={len(fail)}")


if __name__ == "__main__":
    main()
