"""Convert PNG tile blobs in a GeoPackage/SQLite tile database to JPEG.

This script:
* Modified and Refactored by Richard Avery and with assistance from Blake Dellinger. 
* Copies the full schema and non–tile tables from an input GeoPackage/SQLite DB.
* For each detected tile table, reads PNG tile blobs and re-encodes them as JPEG.
* Optionally filters by zoom level range.
* Optionally skips tiles that contain transparency.

It is intentionally conservative about schema handling and only assumes the
standard `(zoom_level, tile_column, tile_row, tile_data)` tile layout.
"""

from __future__ import annotations

import argparse
import io
import os
import sqlite3
import time
from typing import Dict, List, Optional, Sequence, Tuple
# PIL REQUIRES DEPS
from PIL import Image


# ---------------------------------------------------------------------------
# Basic helpers
# ----------------------------------


REQUIRED_TILE_COLUMNS: Tuple[str, str, str, str] = (
    "zoom_level",
    "tile_column",
    "tile_row",
    "tile_data",
)


def parse_rgb(value: str) -> Tuple[int, int, int]:
    """Parse an "R,G,B" string into a 3-tuple of ints in the range [0, 255].

    Raises ValueError on invalid input.
    """
    parts = [p.strip() for p in value.split(",")]
    if len(parts) != 3:
        raise ValueError(f"Expected R,G,B, got {value!r}")
    r, g, b = (int(p) for p in parts)
    for c in (r, g, b):
        if not (0 <= c <= 255):
            raise ValueError(f"RGB component {c} out of range [0, 255]")
    return r, g, b


def quote_ident(name: str) -> str:
    """Safely quote an SQLite identifier.

    This is deliberately simple and only handles double quotes, which is enough
    for programmatically generated identifiers.
    """
    return '"' + name.replace('"', '""') + '"'


def get_table_columns(conn: sqlite3.Connection, table: str) -> List[str]:
    """Return list of column names for a table."""
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({quote_ident(table)})")
    rows = cur.fetchall()
    return [row[1] for row in rows]


def is_tile_table(conn: sqlite3.Connection, table: str) -> bool:
    """Heuristically decide if a table is a 'tile' table.

    We require the standard `(zoom_level, tile_column, tile_row, tile_data)`
    columns to be present.
    """
    cols = get_table_columns(conn, table)
    return all(c in cols for c in REQUIRED_TILE_COLUMNS)


def list_tables(conn: sqlite3.Connection) -> List[str]:
    """Return the list of user tables in the database."""
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return [row[0] for row in cur.fetchall()]


def copy_schema(src_conn: sqlite3.Connection, dst_conn: sqlite3.Connection) -> None:
    """Copy the schema (CREATE TABLE / INDEX / VIEW / TRIGGER) to dst_conn."""
    cur = src_conn.cursor()
    cur.execute(
        """
        SELECT type, name, tbl_name, sql
        FROM sqlite_master
        WHERE sql IS NOT NULL
        ORDER BY type='table' DESC, type, name
        """
    )
    dst_cur = dst_conn.cursor()
    for type_, name, tbl_name, sql in cur.fetchall():
        # We replay the DDL where possible; some internal objects (eg
        # sqlite_sequence) cannot be recreated and will raise an
        # OperationalError. Skip those rather than aborting.
        try:
            dst_cur.execute(sql)
        except sqlite3.OperationalError as e:
            print(f"Skipping DDL for {name}: {e}")
            continue
    dst_conn.commit()


def copy_table_data(
    src_conn: sqlite3.Connection,
    dst_conn: sqlite3.Connection,
    table: str,
) -> int:
    """Copy all rows from a table verbatim.

    Returns the number of rows inserted.
    """
    cur_src = src_conn.cursor()
    cur_dst = dst_conn.cursor()

    cur_src.execute(f"SELECT * FROM {quote_ident(table)}")
    rows = cur_src.fetchall()
    if not rows:
        return 0

    placeholders = ", ".join(["?"] * len(rows[0]))
    insert_sql = f"INSERT INTO {quote_ident(table)} VALUES ({placeholders})"
    cur_dst.executemany(insert_sql, rows)
    return len(rows)


# ---------------------------------------------------------------------------
# Image conversion
# ----------------------------------


def png_blob_to_jpg_blob(
    png_blob: bytes,
    *,
    quality: int,
    background_rgb: Tuple[int, int, int],
    skip_fully_transparent: bool,
    skip_any_alpha: bool,
) -> Optional[bytes]:
    """Convert a PNG image (as bytes) to JPEG (as bytes).

    * If the PNG is fully transparent and skip_fully_transparent is True, return None.
    * If the PNG has any alpha and skip_any_alpha is True, return None.
    * Otherwise, composite onto background_rgb and encode as JPEG.
    """
    with Image.open(io.BytesIO(png_blob)) as im:
        im.load()  # force loading so we can inspect pixels

        has_alpha = "A" in im.getbands()
        if has_alpha:
            alpha = im.getchannel("A")
            bbox = alpha.getbbox()
            if bbox is None:
                # fully transparent
                if skip_fully_transparent:
                    return None
            if skip_any_alpha:
                return None

            # RGBA -> RGB with background
            bg = Image.new("RGB", im.size, background_rgb)
            bg.paste(im, mask=alpha)
            rgb = bg
        else:
            if im.mode != "RGB":
                rgb = im.convert("RGB")
            else:
                rgb = im

        out = io.BytesIO()
        rgb.save(out, format="JPEG", quality=quality)
        return out.getvalue()


# ---------------------------------------------------------------------------
# Query helpers
# ----------------------------------


def build_zoom_filter(
    min_zoom: Optional[int],
    max_zoom: Optional[int],
) -> Tuple[str, Sequence[int]]:
    """Build WHERE clause and parameter list for zoom range filtering."""
    where_clauses: List[str] = []
    params: List[int] = []

    if min_zoom is not None:
        where_clauses.append("zoom_level >= ?")
        params.append(min_zoom)

    if max_zoom is not None:
        where_clauses.append("zoom_level <= ?")
        params.append(max_zoom)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    return where_sql, params


def table_tile_count(
    conn: sqlite3.Connection,
    table: str,
    where_sql: str,
    params: Sequence[int],
) -> int:
    """Return the number of rows (tiles) in a tile table, after filters."""
    cur = conn.cursor()
    cur.execute(
        f"SELECT COUNT(*) FROM {quote_ident(table)} {where_sql}",
        list(params),
    )
    row = cur.fetchone()
    return int(row[0]) if row else 0


def convert_tile_table(
    conn_src: sqlite3.Connection,
    conn_dst: sqlite3.Connection,
    table: str,
    *,
    where_sql: str,
    params: Sequence[int],
    quality: int,
    background: Tuple[int, int, int],
    skip_fully_transparent: bool,
    skip_any_alpha: bool,
    batch_size: int = 1000,
) -> Dict[str, int]:
    """Convert PNG tiles to JPEG inside a single tile table.

    The destination table is truncated before inserting JPEG data.

    This version also reports progress with an ETA and tiles/second rate.
    """
    # Count tiles after filters so we have a total for progress / ETA.
    total = table_tile_count(conn_src, table, where_sql, params)

    print(f"Converting table '{table}' PNG to JPG tiles.")
    print(f"  Source tiles (filtered): {total}")

    src = conn_src.cursor()
    src.execute(
        f"SELECT zoom_level, tile_column, tile_row, tile_data "
        f"FROM {quote_ident(table)} {where_sql}",
        list(params),
    )

    dst = conn_dst.cursor()
    # Truncate destination table before inserting JPEG tiles.
    dst.execute(f"DELETE FROM {quote_ident(table)}")

    insert_sql = (
        f"INSERT INTO {quote_ident(table)} "
        "(zoom_level, tile_column, tile_row, tile_data) VALUES (?, ?, ?, ?)"
    )

    processed_count = 0
    converted_count = 0
    skipped_count = 0

    batch: List[Tuple[int, int, int, sqlite3.Binary]] = []

    start_time = time.time()
    last_report = start_time

    for z, x, y, png_data in src:
        processed_count += 1

        jpg_data = png_blob_to_jpg_blob(
            png_data,
            quality=quality,
            background_rgb=background,
            skip_fully_transparent=skip_fully_transparent,
            skip_any_alpha=skip_any_alpha,
        )

        if jpg_data is None:
            skipped_count += 1
        else:
            converted_count += 1
            batch.append((z, x, y, sqlite3.Binary(jpg_data)))

        if len(batch) >= batch_size:
            dst.executemany(insert_sql, batch)
            conn_dst.commit()
            batch = []

        # Progress output throttled to about once per second.
        now = time.time()
        if now - last_report >= 1.0:
            elapsed = now - start_time
            rate = processed_count / elapsed if elapsed > 0 else 0.0

            if total:
                pct = (processed_count / total) * 100.0
                remaining = max(total - processed_count, 0)
                eta_sec = remaining / rate if rate > 0 else 0.0
                eta_min = eta_sec / 60.0
                msg = (
                    f"  {table}: {processed_count}/{total} "
                    f"({pct:5.1f}%) | converted={converted_count}, "
                    f"skipped={skipped_count} | {rate:7.1f} tiles/s "
                    f"| ETA {eta_min:5.1f} min"
                )
            else:
                msg = (
                    f"  {table}: processed {processed_count} tiles "
                    f"(converted={converted_count}, skipped={skipped_count})"
                )

            print(msg, end="\r", flush=True)
            last_report = now

    # Flush remaining batch.
    if batch:
        dst.executemany(insert_sql, batch)
        conn_dst.commit()

    # Final newline so the next prints don't overwrite the progress line.
    elapsed = time.time() - start_time
    rate = processed_count / elapsed if elapsed > 0 else 0.0
    if total:
        print(
            f"\n  Finished {table}: {processed_count}/{total} tiles "
            f"(converted={converted_count}, skipped={skipped_count}) "
            f"in {elapsed:.1f}s ({rate:7.1f} tiles/s)"
        )
    else:
        print(
            f"\n  Finished {table}: {processed_count} tiles "
            f"(converted={converted_count}, skipped={skipped_count}) "
            f"in {elapsed:.1f}s ({rate:7.1f} tiles/s)"
        )

    return {
        "total": total,
        "processed": processed_count,
        "converted": converted_count,
        "skipped": skipped_count,
    }


# ------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description=(
            "Copy a tile GeoPackage/SQLite DB and convert PNG tile_data blobs "
            "to JPEG with optional alpha skipping."
        )
    )
    p.add_argument(
        "input_gpkg",
        help="Input GeoPackage/SQLite tile DB path",
    )
    p.add_argument(
        "output",
        help="Output GeoPackage/SQLite path (must not already exist unless --force is used)",
    )
    p.add_argument(
        "--quality",
        type=int,
        default=90,
        help="JPEG quality (1–100, default=90)",
    )
    p.add_argument(
        "--min-zoom",
        type=int,
        dest="min_zoom",
        default=None,
        help="Minimum zoom_level to process (inclusive)",
    )
    p.add_argument(
        "--max-zoom",
        type=int,
        dest="max_zoom",
        default=None,
        help="Maximum zoom_level to process (inclusive)",
    )
    p.add_argument(
        "--skip-fully-transparent",
        action="store_true",
        help="Skip tiles that are fully transparent",
    )
    p.add_argument(
        "--skip-any-alpha",
        action="store_true",
        help="Skip tiles that have any alpha at all",
    )
    p.add_argument(
        "--background",
        type=parse_rgb,
        default="0,0,0",
        help='Background RGB as "R,G,B" for compositing alpha (default "0,0,0")',
    )
    p.add_argument(
        "--force",
        action="store_true",
        help="Overwrite output if it exists",
    )
    return p


def main(argv: Optional[Sequence[str]] = None) -> None:
    parser = build_arg_parser()
    args = parser.parse_args(argv)

    in_path = args.input_gpkg
    out_path = args.output

    if os.path.abspath(in_path) == os.path.abspath(out_path):
        raise SystemExit("Input and output paths must be different.")

    if os.path.exists(out_path):
        if not args.force:
            raise SystemExit(f"Output exists: {out_path}. Use --force to overwrite.")
        os.remove(out_path)

    if not (1 <= args.quality <= 100):
        raise SystemExit("JPEG quality must be in [1, 100].")

    background = args.background
    skip_fully_transparent = args.skip_fully_transparent

    print("Configuration:")
    print(f"  Input DB:  {in_path}")
    print(f"  Output DB: {out_path}")
    print(f"  JPEG quality: {args.quality}")
    print(f"  skip_fully_transparent: {skip_fully_transparent}")
    print(f"  skip_any_alpha: {args.skip_any_alpha}")
    print(f"  alpha composite background: {background}")
    if args.min_zoom is not None or args.max_zoom is not None:
        print(f"  Zoom filter: {args.min_zoom} to {args.max_zoom}")
    print()

    # Open original DB read-only; create new DB.
    conn_orig = sqlite3.connect(f"file:{in_path}?mode=ro", uri=True)
    conn_new = sqlite3.connect(out_path)

    try:
        print("Copying schema...")
        copy_schema(conn_orig, conn_new)

        # Detect tile tables.
        tables = list_tables(conn_orig)
        tile_tables = [t for t in tables if is_tile_table(conn_orig, t)]
        print(f"Detected {len(tile_tables)} tile tables.")
        if tile_tables:
            print(f"  Tile tables to convert: {', '.join(tile_tables)}")

        # Build zoom filter.
        where_sql, params = build_zoom_filter(args.min_zoom, args.max_zoom)

        # Copy non-tile tables as-is.
        all_tables = list_tables(conn_orig)
        metadata_tables = [t for t in all_tables if t not in tile_tables]
        print(f"Copying data from {len(metadata_tables)} metadata tables.")
        for table in metadata_tables:
            print(f"  - Copying {table}...")
            copied = copy_table_data(conn_orig, conn_new, table)
            if copied:
                print(f"    rows copied: {copied}")
        conn_new.commit()

        total_in = 0
        total_converted = 0
        total_skipped = 0

        for table in tile_tables:
            stats = convert_tile_table(
                conn_src=conn_orig,
                conn_dst=conn_new,
                table=table,
                where_sql=where_sql,
                params=params,
                quality=args.quality,
                background=background,
                skip_fully_transparent=skip_fully_transparent,
                skip_any_alpha=args.skip_any_alpha,
            )
            total_in += stats["total"]
            total_converted += stats["converted"]
            total_skipped += stats["skipped"]

        print("\nConversion complete")
        print(f"  Input:  {in_path}")
        print(f"  Output: {out_path}")
        print(f"  Tile tables converted: {len(tile_tables)}")
        print(f"  Source tiles (filtered): {total_in}")
        print(f"  Tiles converted to JPG: {total_converted}")
        print(f"  Tiles skipped: {total_skipped}")
        print(f"  JPG quality: {args.quality}")
        print(f"  skip_any_alpha: {args.skip_any_alpha}")
        print(f"  alpha composite background: {background}")
        if args.min_zoom is not None or args.max_zoom is not None:
            print(f"  Zoom filter: {args.min_zoom} to {args.max_zoom}")

        print("\nDone")

    finally:
        conn_orig.close()
        conn_new.close()


if __name__ == "__main__":
    main()