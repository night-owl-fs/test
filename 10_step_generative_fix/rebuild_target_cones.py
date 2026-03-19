#!/usr/bin/env python3
import argparse
import json
import math
import re
import subprocess
import tempfile
from pathlib import Path

WEBM_HALF = 20037508.342789244
WEBM_WORLD = WEBM_HALF * 2.0
AIRPORT_FOLDER_ALIASES = {"KCRQ": "KRCQ"}
FNAME_RE = re.compile(
    r"Z(?P<out>\d+)_fromZ(?P<base>\d+)_r(?P<r>\d+)_c(?P<c>\d+)_baseX(?P<bx>\d+)_baseY(?P<by>\d+)\.tif$"
)


def run(cmd):
    p = subprocess.run(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    return p.returncode == 0, p.stdout


def to_win_path(p: Path | str) -> str:
    s = str(p)
    if s.startswith("/"):
        out = subprocess.run(["wslpath", "-w", s], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
        if out.returncode == 0:
            return out.stdout.strip()
    return s


def resolve_gdal(name: str) -> str:
    for base in (Path("/mnt/c/OSGeo4W/bin"), Path(r"C:\OSGeo4W\bin")):
        p = base / name
        if p.exists():
            return str(p)
    return name


def valid_png(path: Path) -> bool:
    return path.exists() and path.stat().st_size > 100


def collect_airport_dirs(root: Path):
    return sorted([p for p in root.iterdir() if p.is_dir() and re.match(r"^K[A-Z0-9]{3}$", p.name)])


def best_source_tile(root: Path, preferred_airport_dir: Path, z: int, x: int, y: int, airport_dirs: list[Path]):
    rel = Path(str(z)) / f"{x}_{y}.png"
    candidates = []
    p = preferred_airport_dir / rel
    if valid_png(p):
        candidates.append(p)
    for ap in airport_dirs:
        if ap == preferred_airport_dir:
            continue
        q = ap / rel
        if valid_png(q):
            candidates.append(q)
    if not candidates:
        return None
    return max(candidates, key=lambda t: t.stat().st_size)


def quality_score(tif_path: Path):
    gdalinfo = resolve_gdal("gdalinfo.exe")
    ok, out = run([gdalinfo, "-json", "-stats", to_win_path(tif_path)])
    if not ok:
        return -1.0
    try:
        j = json.loads(out)
        bands = j.get("bands", [])
        if not bands:
            return -1.0
        score = 0.0
        for b in bands[:3]:
            md = (b.get("metadata") or {}).get("", {})
            mn = float(md.get("STATISTICS_MINIMUM", "0"))
            mx = float(md.get("STATISTICS_MAXIMUM", "0"))
            mean = float(md.get("STATISTICS_MEAN", "0"))
            std = float(md.get("STATISTICS_STDDEV", "0"))
            score += (std * 4.0) + ((mx - mn) * 0.2) + (mean * 0.05)
        score += min(tif_path.stat().st_size / 1_000_000.0, 200.0)
        return score
    except Exception:
        return -1.0


def cone_bounds(out_z: int, base_z: int, bx: int, by: int):
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


def source_tile_range(minx: float, miny: float, maxx: float, maxy: float, zsrc: int):
    n = 1 << zsrc
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


def build_from_zoom(root: Path, airport_dir: Path, airport_dirs: list[Path], tif_path: Path, out_z: int, base_z: int, bx: int, by: int, src_z: int):
    minx, miny, maxx, maxy, factor = cone_bounds(out_z, base_z, bx, by)
    x_min, x_max, y_min, y_max = source_tile_range(minx, miny, maxx, maxy, src_z)
    src_dir = airport_dir / str(src_z)
    if not src_dir.exists():
        return False, f"missing zoom dir {src_dir}"

    srcs = []
    for y in range(y_min, y_max + 1):
        for x in range(x_min, x_max + 1):
            p = best_source_tile(root, airport_dir, src_z, x, y, airport_dirs)
            if p is None:
                return False, f"missing/corrupt source tile {x}_{y}.png at Z{src_z}"
            srcs.append(p)

    gdalbuildvrt = resolve_gdal("gdalbuildvrt.exe")
    gdal_translate = resolve_gdal("gdal_translate.exe")
    out_px = factor * 256
    out_vrt = tif_path.with_suffix(".vrt")
    out_tmp = tif_path

    with tempfile.NamedTemporaryFile("w", suffix=".txt", delete=False, encoding="utf-8") as f:
        list_path = Path(f.name)
        for p in srcs:
            f.write(to_win_path(p) + "\n")

    ok, out = run([gdalbuildvrt, "-q", "-input_file_list", to_win_path(list_path), to_win_path(out_vrt)])
    list_path.unlink(missing_ok=True)
    if not ok:
        return False, out

    ok, out = run(
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
            to_win_path(out_tmp),
        ]
    )
    if not ok:
        out_tmp.unlink(missing_ok=True)
        return False, out

    return True, f"from Z{src_z}"


def parse_target(path: Path):
    m = FNAME_RE.search(path.name)
    if not m:
        raise ValueError(f"Bad filename pattern: {path.name}")
    return {
        "out_z": int(m.group("out")),
        "base_z": int(m.group("base")),
        "bx": int(m.group("bx")),
        "by": int(m.group("by")),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--root", default=".")
    ap.add_argument("--file", action="append", help="Target TIFF path to rebuild.")
    ap.add_argument("--file-list", help="Path to text file with one target TIFF path per line.")
    ap.add_argument("--best-size", action="store_true", help="Try all candidate zooms and keep largest output.")
    ap.add_argument("--force-src-zoom", action="append", type=int, help="Restrict source zoom(s), e.g. --force-src-zoom 13")
    args = ap.parse_args()

    root = Path(args.root).resolve()
    targets = [Path(f) for f in (args.file or [])]
    if args.file_list:
        fl = Path(args.file_list).resolve()
        for line in fl.read_text(encoding="utf-8", errors="replace").splitlines():
            s = line.strip()
            if s:
                targets.append(Path(s))
    if not targets:
        raise SystemExit("No targets provided. Use --file and/or --file-list.")
    airport_dirs = collect_airport_dirs(root)
    rebuilt = 0
    failed = 0

    for tif in targets:
        tif = (root / tif).resolve() if not tif.is_absolute() else tif.resolve()
        meta = parse_target(tif)
        airport = tif.parent.parent.name
        airport_dir = root / airport
        if not airport_dir.exists() and airport in AIRPORT_FOLDER_ALIASES:
            airport_dir = root / AIRPORT_FOLDER_ALIASES[airport]
        out_z = meta["out_z"]
        candidates = [out_z + 1, out_z + 2, out_z - 1, out_z + 3, out_z - 2, out_z + 4, out_z - 3]
        candidates = [z for z in candidates if 13 <= z <= 18 and z != out_z]
        if args.force_src_zoom:
            forced = [z for z in args.force_src_zoom if 13 <= z <= 18 and z != out_z]
            candidates = [z for z in forced if z in candidates or True]

        if args.best_size:
            best_path = None
            best_score = -1.0
            best_src = None
            for src_z in candidates:
                cand = tif.with_name(f"{tif.stem}.src{src_z}.tif")
                ok, _ = build_from_zoom(
                    root=root,
                    airport_dir=airport_dir,
                    airport_dirs=airport_dirs,
                    tif_path=cand,
                    out_z=meta["out_z"],
                    base_z=meta["base_z"],
                    bx=meta["bx"],
                    by=meta["by"],
                    src_z=src_z,
                )
                if ok and cand.exists():
                    score = quality_score(cand)
                    if score > best_score:
                        best_score = score
                        best_path = cand
                        best_src = src_z
                elif cand.exists():
                    cand.unlink(missing_ok=True)
                cand_vrt = cand.with_suffix(".vrt")
                cand_vrt.unlink(missing_ok=True)

            if best_path is None:
                failed += 1
                print(f"[FAIL] {tif}")
            else:
                best_path.replace(tif)
                print(f"[OK] {tif} from Z{best_src} score={best_score:.2f}")
                rebuilt += 1
                for src_z in candidates:
                    c = tif.with_name(f"{tif.stem}.src{src_z}.tif")
                    c.unlink(missing_ok=True)
                    c.with_suffix(".vrt").unlink(missing_ok=True)
        else:
            ok_any = False
            for src_z in candidates:
                tmp = tif.with_name(f"{tif.stem}.tmp.tif")
                ok, msg = build_from_zoom(
                    root=root,
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
                    print(f"[OK] {tif} {msg}")
                    rebuilt += 1
                    ok_any = True
                    break
                tmp.unlink(missing_ok=True)
                tmp.with_suffix(".vrt").unlink(missing_ok=True)
            if not ok_any:
                failed += 1
                print(f"[FAIL] {tif}")

    print(f"\nDone. rebuilt={rebuilt} failed={failed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
