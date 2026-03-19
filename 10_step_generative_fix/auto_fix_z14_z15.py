#!/usr/bin/env python3
import argparse
import re
import subprocess
import sys
import time
from pathlib import Path


ISSUE_RE = re.compile(
    r"^- (K\w+) Z(14|15) r(\d+) c(\d+) baseX(\d+) baseY(\d+): (.+)$"
)


def run(cmd, cwd: Path):
    p = subprocess.run(
        cmd,
        cwd=str(cwd),
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    return p.returncode, p.stdout


def parse_report_for_targets(report_path: Path):
    targets = []
    if not report_path.exists():
        return targets
    for line in report_path.read_text(encoding="utf-8", errors="replace").splitlines():
        m = ISSUE_RE.match(line.strip())
        if not m:
            continue
        ap, z, r, c, bx, by, _reason = m.groups()
        tif = (
            f"CODE/CONE_GEOTIFF_OUTPUT/{ap}/Z{z}/"
            f"Z{z}_fromZ9_r{r}_c{c}_baseX{bx}_baseY{by}.tif"
        )
        targets.append(tif)
    # de-dup while preserving order
    seen = set()
    out = []
    for t in targets:
        if t not in seen:
            seen.add(t)
            out.append(t)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--sleep-seconds", type=int, default=90)
    ap.add_argument("--once", action="store_true")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    reports_dir = root / "CODE" / "reports"
    reports_dir.mkdir(parents=True, exist_ok=True)
    report = reports_dir / "cone_quality_scan_report.md"
    target_list = reports_dir / "z14_z15_autofix_targets.txt"
    log = reports_dir / "z14_z15_autofix.log"

    iteration = 0
    while True:
        iteration += 1
        ts = time.strftime("%Y-%m-%d %H:%M:%S")
        with log.open("a", encoding="utf-8") as lf:
            lf.write(f"\n[{ts}] iteration={iteration} scan start\n")

        code, out = run(
            [py, "CODE/scan_all_cones_and_build_download_list.py", "--root", "."],
            cwd=root,
        )
        with log.open("a", encoding="utf-8") as lf:
            lf.write(out + "\n")
        if code != 0:
            with log.open("a", encoding="utf-8") as lf:
                lf.write(f"[{ts}] scan failed code={code}, sleeping\n")
            if args.once:
                return code
            time.sleep(args.sleep_seconds)
            continue

        targets = parse_report_for_targets(report)
        target_list.write_text("".join(t + "\n" for t in targets), encoding="utf-8")
        with log.open("a", encoding="utf-8") as lf:
            lf.write(f"[{ts}] targets_z14_z15={len(targets)}\n")

        if not targets:
            with log.open("a", encoding="utf-8") as lf:
                lf.write(f"[{ts}] no z14/z15 issues found\n")
            if args.once:
                return 0
            time.sleep(args.sleep_seconds)
            continue

        code, out = run(
            [
                py,
                "-u",
                "CODE/rebuild_target_cones.py",
                "--best-size",
                "--root",
                ".",
                "--file-list",
                str(target_list),
            ],
            cwd=root,
        )
        with log.open("a", encoding="utf-8") as lf:
            lf.write(out + "\n")
            lf.write(f"[{ts}] rebuild_exit_code={code}\n")

        if args.once:
            return code
        time.sleep(args.sleep_seconds)


if __name__ == "__main__":
    raise SystemExit(main())
    py = sys.executable or "/usr/bin/python3"
