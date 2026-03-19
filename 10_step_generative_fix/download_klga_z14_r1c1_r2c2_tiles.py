#!/usr/bin/env python3
from __future__ import annotations

import argparse
import pathlib
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed


def build_url(template: str, airport: str, z: int, x: int, y: int, ext: str) -> str:
    rel = f"{airport}/{z}/{x}_{y}.{ext}"
    return template.format(airport=airport, z=z, x=x, y=y, ext=ext, path=rel)


def is_valid(path: pathlib.Path, min_bytes: int) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size >= min_bytes


def download(url: str, out: pathlib.Path, retries: int, timeout: int) -> tuple[bool, str]:
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".part")
    err = ""
    for i in range(retries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "klga-r1c1-r2c2-fix/1.0"})
            with urllib.request.urlopen(req, timeout=timeout) as r, tmp.open("wb") as f:
                f.write(r.read())
            tmp.replace(out)
            return True, ""
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, OSError) as e:
            err = str(e)
            tmp.unlink(missing_ok=True)
            if i + 1 < retries:
                time.sleep(1.2)
    return False, err


def add_range(jobs, airport, z, x_min, x_max, y_min, y_max, root, include_pgw):
    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            png = root / airport / str(z) / f"{x}_{y}.png"
            jobs.append((airport, z, x, y, "png", png, 100))
            if include_pgw:
                pgw = root / airport / str(z) / f"{x}_{y}.pgw"
                jobs.append((airport, z, x, y, "pgw", pgw, 20))


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Download replacement tiles for KLGA Z14 gray issues: r1c1 and r2c2 (plus Z15 fallback ranges)."
    )
    ap.add_argument("--url-template", required=True, help="Placeholders: {airport} {z} {x} {y} {ext} {path}")
    ap.add_argument("--root", default=r"C:\Users\ravery\Downloads\AIRPORT_TILES")
    ap.add_argument("--workers", type=int, default=24)
    ap.add_argument("--retries", type=int, default=4)
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--include-pgw", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    root = pathlib.Path(args.root)
    jobs = []

    # Z14_fromZ9_r1_c1_baseX150_baseY192
    add_range(jobs, "KLGA", 14, 4800, 4831, 6144, 6175, root, args.include_pgw)
    # Z14_fromZ9_r2_c2_baseX151_baseY193
    add_range(jobs, "KLGA", 14, 4832, 4863, 6176, 6207, root, args.include_pgw)

    # Matching Z15 fallback windows for rebuilding Z14 from Z15
    add_range(jobs, "KLGA", 15, 9600, 9663, 12288, 12351, root, args.include_pgw)
    add_range(jobs, "KLGA", 15, 9664, 9727, 12352, 12415, root, args.include_pgw)

    print(f"Planned files: {len(jobs)}")
    if args.dry_run:
        for j in jobs[:30]:
            airport, z, x, y, ext, out, _ = j
            print(build_url(args.url_template, airport, z, x, y, ext), "->", out)
        if len(jobs) > 30:
            print(f"... and {len(jobs) - 30} more")
        return 0

    ok = 0
    fails = []

    def worker(job):
        airport, z, x, y, ext, out, min_b = job
        url = build_url(args.url_template, airport, z, x, y, ext)
        good, err = download(url, out, args.retries, args.timeout)
        if not good:
            return False, f"{url} -> {out}: {err}"
        if not is_valid(out, min_b):
            return False, f"too small: {out}"
        return True, ""

    with ThreadPoolExecutor(max_workers=args.workers) as ex:
        futs = [ex.submit(worker, j) for j in jobs]
        for i, fut in enumerate(as_completed(futs), 1):
            good, msg = fut.result()
            if good:
                ok += 1
            else:
                fails.append(msg)
            if i % 250 == 0 or i == len(futs):
                print(f"Progress {i}/{len(futs)} ok={ok} fail={len(fails)}")

    if fails:
        rep = root / "CODE" / "reports" / "klga_z14_r1c1_r2c2_download_failures.txt"
        rep.parent.mkdir(parents=True, exist_ok=True)
        rep.write_text("\n".join(fails) + "\n", encoding="utf-8")
        print(f"Failures: {rep}")
        return 2

    print("Download replacement complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
