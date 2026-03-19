#!/usr/bin/env python3
"""
Merge multiple MBTiles/GeoPackage raster tile SQLite files into one.
Supports non-overlapping and overlapping tiles (with priority: last file wins). (LIAR LIAR PANTS ON FIRE!)
Includes background-tile skipping (for JPG with solid background color).
"""

import sqlite3
import sys
import os
import time
import argparse
import shutil
import math
import io
from PIL import Image
import numpy as np
import subprocess
from typing import List, Tuple, Dict, Any


def get_db_type(conn) -> str:
    """Detect if MBTiles or GeoPackage"""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cur.fetchall()]
    if 'metadata' in tables and 'tiles' in tables:
        return 'mbtiles'
    elif any(t.startswith('gpkg_') for t in tables) and 'tiles' in tables:
        return 'geopackage'
    else:
        raise ValueError("Unknown tile format")


def get_metadata(conn) -> Dict[str, str]:
    """Extract metadata name-value pairs"""
    cur = conn.cursor()
    try:
        cur.execute("SELECT name, value FROM metadata")
        return {row[0]: row[1] for row in cur.fetchall()}
    except sqlite3.OperationalError:
        return {}


def get_gpkg_contents(conn) -> List[Dict[str, Any]]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id FROM gpkg_contents")
        keys = [description[0] for description in cur.description]
        return [dict(zip(keys, row)) for row in cur.fetchall()]
    except sqlite3.OperationalError:
        return []


def get_zoom_levels(conn) -> Tuple[int, int]:
    cur = conn.cursor()
    try:
        cur.execute("SELECT MIN(zoom_level), MAX(zoom_level) FROM tiles")
        row = cur.fetchone()
        return (row[0] or 0, row[1] or 0)
    except:
        return (0, 0)


def lonlat_to_meters(lon: float, lat: float) -> Tuple[float, float]:
    """Convert lon/lat (degrees) to Web Mercator (EPSG:3857) in meters"""
    x = lon * 20037508.34 / 180
    y = math.log(math.tan((90 + lat) * math.pi / 360)) / (math.pi / 180)
    y = y * 20037508.34 / 180
    return x, y


def get_tile_bounds(conn) -> Tuple[float, float, float, float]:
    """Get bounds in lat/lon from metadata or compute from tiles"""
    meta = get_metadata(conn)
    if 'bounds' in meta:
        try:
            parts = [float(x) for x in meta['bounds'].split(',')]
            if len(parts) == 4:
                return tuple(parts)
        except:
            pass

    # Fallback: compute from tile coordinates (returns lat/lon)
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT MIN(tile_column), MAX(tile_column), MIN(tile_row), MAX(tile_row), zoom_level
            FROM tiles GROUP BY zoom_level
        """)
        min_x = float('inf')
        max_x = float('-inf')
        min_y = float('inf')
        max_y = float('-inf')

        for min_col, max_col, min_r, max_r, z in cur.fetchall():
            n = 2 ** z
            lon1 = min_col / n * 360.0 - 180.0
            lon2 = (max_col + 1) / n * 360.0 - 180.0
            # Corrected latitude calculation
            lat_north = 180 / math.pi * (2 * math.atan(math.exp(math.pi * (1 - 2 * min_r / n))) - math.pi / 2)
            lat_south = 180 / math.pi * (2 * math.atan(math.exp(math.pi * (1 - 2 * (max_r + 1) / n))) - math.pi / 2)
            min_x = min(min_x, lon1)
            max_x = max(max_x, lon2)
            min_y = min(min_y, lat_south)
            max_y = max(max_y, lat_north)

        return (min_x, min_y, max_x, max_y) if min_x != float('inf') else (-180, -85, 180, 85)
    except:
        return (-180, -85, 180, 85)


def is_background(blob: bytes,
                  background: Tuple[int, int, int],
                  threshold: float = 5.0,
                  percentage: float = 0.95,
                  variance_threshold: float = 10.0) -> bool:
    """
    Tile is background if:
      - variance is low (nearly uniform), AND
      - >percentage pixels are within 'threshold' distance of background RGB.
    Designed to tolerate JPEG artifacts.
    """
    try:
        img = Image.open(io.BytesIO(blob))
        arr = np.array(img)
        if arr.ndim == 2:  # Grayscale to RGB
            arr = np.dstack((arr, arr, arr))
        variance = np.var(arr)
        if variance > variance_threshold:
            return False  # High variance = not uniform
        dist = np.linalg.norm(arr - np.array(background), axis=-1)
        close_pixels = dist < threshold
        close_ratio = np.mean(close_pixels)
        return close_ratio > percentage
    except:
        return False  # If not an image or error, treat as not background


def cleanup_primary_background_tiles(conn,
                                     background: Tuple[int, int, int],
                                     threshold: float,
                                     percentage: float,
                                     variance_threshold: float):
    """
    Remove tiles from the primary DB that are 'just background'
    (e.g. solid black JPG padding tiles).
    """
    print("DEBUG: Cleaning background-only tiles from primary...")
    cur = conn.cursor()
    cur.execute("SELECT rowid, tile_data FROM tiles")
    rows = cur.fetchall()

    to_delete = []
    for rowid, blob in rows:
        if is_background(blob, background, threshold, percentage, variance_threshold):
            to_delete.append((rowid,))

    if to_delete:
        cur.executemany("DELETE FROM tiles WHERE rowid = ?", to_delete)
        conn.commit()
        print(f"DEBUG: Deleted {len(to_delete)} background-only tiles from primary.")
    else:
        print("DEBUG: No background-only tiles detected in primary.")


def add_overviews(output_file: str):
    """Add overviews using GDALaddo for low-zoom levels."""
    cmd = ['gdaladdo', '-r', 'average', '-ro', f"GPKG:{output_file}:tiles", '2', '4', '8', '16']
    print(f"DEBUG: Running GDALaddo: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        print("DEBUG: Overviews added successfully.")
    except Exception as e:
        print(f"Warning: GDALaddo failed: {e}. Install GDAL or run manually.")


def merge_databases(input_files: List[str],
                    output_file: str,
                    overwrite: bool = False,
                    background_rgb: str = "255,255,255",
                    threshold: float = 5.0,
                    percentage: float = 0.95,
                    variance_threshold: float = 10.0,
                    add_overviews_flag: bool = False):

    if len(input_files) < 2:
        print("Error: at least 2 input files required.")
        return

    if os.path.exists(output_file) and not overwrite:
        print(f"Error: Output file '{output_file}' exists. Use --force to overwrite.")
        return

    # Parse background color
    try:
        background = tuple(map(int, background_rgb.split(',')))
        if len(background) != 3:
            raise ValueError
    except ValueError:
        print("Error: --background_rgb must be 'R,G,B' (e.g., '255,255,255')")
        sys.exit(1)
    print(f"DEBUG: Background RGB to skip: {background}, threshold: {threshold}, percentage: {percentage}, variance_threshold: {variance_threshold}")

    # Copy primary file as base
    primary = input_files[0]
    print(f"DEBUG: Copying primary file as base: {primary}")
    shutil.copy2(primary, output_file)

    output_file = os.path.abspath(output_file).replace('\\', '/')
    input_files = [os.path.abspath(f).replace('\\', '/') for f in input_files]

    conn = sqlite3.connect(f"file:{output_file}?mode=rw", uri=True)
    conn.execute("PRAGMA journal_mode = OFF;")
    conn.execute("PRAGMA synchronous = OFF;")
    conn.execute("PRAGMA cache_size = -20000;")
    conn.execute("PRAGMA temp_store = MEMORY;")
    conn.execute("BEGIN;")

    db_type = get_db_type(conn)
    print(f"Detected format: {db_type.upper()}")

    global_minzoom = float('inf')
    global_maxzoom = float('-inf')
    global_bounds_latlon = [180, 85, -180, -85]  # minx, miny, maxx, maxy in degrees

    cur = conn.cursor()
    cur.execute("PRAGMA table_info(tiles);")
    target_cols = [row[1] for row in cur.fetchall()]
    print(f"Target tiles columns: {target_cols}")

    def update_bounds(current, new_bounds):
        return [
            min(current[0], new_bounds[0]),
            min(current[1], new_bounds[1]),
            max(current[2], new_bounds[2]),
            max(current[3], new_bounds[3])
        ]

    try:
        # Process primary (already in output)
        print(f"[1/{len(input_files)}] Processing primary (tiles already copied): {input_files[0]}")
        minz, maxz = get_zoom_levels(conn)
        global_minzoom = min(global_minzoom, minz)
        global_maxzoom = max(global_maxzoom, maxz)
        print(f"DEBUG: Zooms from primary: min={minz}, max={maxz}")
        bounds = get_tile_bounds(conn)
        global_bounds_latlon = update_bounds(global_bounds_latlon, bounds)
        print(f"DEBUG: Bounds from primary: {', '.join(f'{x:.6f}' for x in bounds)}")

        # Clean background-only tiles from primary
        cleanup_primary_background_tiles(conn, background, threshold, percentage, variance_threshold)

        # Merge metadata from all sources (union, last wins for duplicates)
        all_meta = get_metadata(conn)  # Start with primary
        for filepath in input_files[1:]:
            print(f"DEBUG: Extracting metadata from {filepath}")
            aux_conn = sqlite3.connect(f"file:{filepath}?mode=ro", uri=True)
            aux_meta = get_metadata(aux_conn)
            print(f"DEBUG: Metadata keys from {filepath}: {list(aux_meta.keys())}")
            all_meta.update(aux_meta)  # Last file wins for key conflicts
            aux_conn.close()

        # (Optional) write unioned metadata back for mbtiles only
        # For GeoPackage we rely on gpkg_* tables.

        # Merge secondaries
        for i in range(1, len(input_files)):
            filepath = input_files[i]
            alias = f"aux{i}"
            print(f"[{i+1}/{len(input_files)}] Merging: {filepath}")

            for attempt in range(3):
                aux_conn = None
                try:
                    aux_conn = sqlite3.connect(f"file:{filepath}?mode=ro&immutable=1", uri=True)
                    aux_conn.execute("PRAGMA query_only = ON;")
                    print(f"DEBUG: Successfully opened and attached {filepath}")

                    # Zoom
                    minz, maxz = get_zoom_levels(aux_conn)
                    global_minzoom = min(global_minzoom, minz)
                    global_maxzoom = max(global_maxzoom, maxz)
                    print(f"DEBUG: Zooms from {filepath}: min={minz}, max={maxz}")

                    # Bounds (lat/lon)
                    bounds = get_tile_bounds(aux_conn)
                    global_bounds_latlon = update_bounds(global_bounds_latlon, bounds)
                    print(f"DEBUG: Bounds from {filepath}: {', '.join(f'{x:.6f}' for x in bounds)}")

                    # Attach
                    conn.execute(f"ATTACH DATABASE 'file:{filepath}?mode=ro&immutable=1' AS {alias}")

                    # Insert non-background tiles only
                    aux_cur = aux_conn.cursor()
                    aux_cur.execute("SELECT zoom_level, tile_column, tile_row, tile_data FROM tiles")
                    insert_count = 0
                    background_count = 0
                    for zoom, col, row_num, data in aux_cur:
                        if not is_background(data, background, threshold, percentage, variance_threshold):
                            conn.execute(
                                "INSERT OR REPLACE INTO tiles (zoom_level, tile_column, tile_row, tile_data) "
                                "VALUES (?, ?, ?, ?)",
                                (zoom, col, row_num, data)
                            )
                            insert_count += 1
                        else:
                            background_count += 1
                    print(f"DEBUG: From {filepath}: Inserted {insert_count} non-background tiles, skipped {background_count} background tiles")

                    # GEOPACKAGE METADATA
                    if db_type == 'geopackage':
                        # gpkg_tile_matrix
                        try:
                            conn.execute(f"""
                                INSERT OR IGNORE INTO gpkg_tile_matrix 
                                (table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size)
                                SELECT table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size
                                FROM {alias}.gpkg_tile_matrix
                            """)
                            print(f"DEBUG: gpkg_tile_matrix merged from {filepath}")
                        except Exception as e:
                            print(f"   Warning: gpkg_tile_matrix merge failed for {filepath}: {e}")

                        # gpkg_tile_matrix_set (use from first valid, update at end)
                        try:
                            src_cur = aux_conn.cursor()
                            src_cur.execute(
                                f"SELECT min_x, min_y, max_x, max_y FROM {alias}.gpkg_tile_matrix_set WHERE table_name = 'tiles'"
                            )
                            row = src_cur.fetchone()
                            if row:
                                src_minx, src_miny, src_maxx, src_maxy = row
                                conn.execute("""
                                    INSERT OR REPLACE INTO gpkg_tile_matrix_set 
                                    (table_name, srs_id, min_x, min_y, max_x, max_y)
                                    VALUES ('tiles', 3857, ?, ?, ?, ?)
                                """, (src_minx, src_miny, src_maxx, src_maxy))
                                print(f"DEBUG: gpkg_tile_matrix_set merged from {filepath}")
                        except Exception as e:
                            print(f"   Warning: gpkg_tile_matrix_set merge failed for {filepath}: {e}")

                    break  # Success

                except sqlite3.OperationalError as e:
                    if "locked" in str(e).lower() and attempt < 2:
                        print(f"   Lock detected. Retrying in 2s... ({attempt+1}/3)")
                        if aux_conn:
                            try:
                                aux_conn.close()
                            except:
                                pass
                        time.sleep(2)
                        continue
                    else:
                        raise
                finally:
                    if aux_conn:
                        try:
                            aux_conn.close()
                        except:
                            pass
                    try:
                        conn.execute(f"DETACH DATABASE {alias};")
                    except sqlite3.OperationalError as e:
                        if "no such database" not in str(e):
                            print(f"   Warning: DETACH failed for {filepath}: {e}")

        # === FINAL METADATA UPDATE (EPSG:3857) ===
        if db_type == 'geopackage':
            minx_m, miny_m = lonlat_to_meters(global_bounds_latlon[0], global_bounds_latlon[1])
            maxx_m, maxy_m = lonlat_to_meters(global_bounds_latlon[2], global_bounds_latlon[3])
            print(f"DEBUG: Final unioned bounds in meters: {minx_m:.4f}, {miny_m:.4f}, {maxx_m:.4f}, {maxy_m:.4f}")

            # Update gpkg_contents
            contents = get_gpkg_contents(conn)
            if contents:
                c = contents[0]
                conn.execute("""
                    UPDATE gpkg_contents SET 
                    min_x=?, min_y=?, max_x=?, max_y=?, last_change=?
                    WHERE table_name=?
                """, (minx_m, miny_m, maxx_m, maxy_m,
                      sqlite3.datetime.datetime.now().isoformat(), c['table_name']))
                print("DEBUG: gpkg_contents updated with final bounds")

            # Final gpkg_tile_matrix_set (union bounds)
            conn.execute("""
                INSERT OR REPLACE INTO gpkg_tile_matrix_set 
                (table_name, srs_id, min_x, min_y, max_x, max_y)
                VALUES ('tiles', 3857, ?, ?, ?, ?)
            """, (minx_m, miny_m, maxx_m, maxy_m))
            print("DEBUG: gpkg_tile_matrix_set updated with final bounds")

            # Ensure gpkg_tile_matrix has entries for all zooms (standard values)
            for zoom in range(int(global_minzoom), int(global_maxzoom) + 1):
                matrix_size = 2 ** zoom
                pixel_size = 156543.03392804097 / (2 ** zoom)
                conn.execute("""
                    INSERT OR IGNORE INTO gpkg_tile_matrix 
                    (table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size)
                    VALUES ('tiles', ?, ?, ?, 256, 256, ?, ?)
                """, (zoom, matrix_size, matrix_size, pixel_size, pixel_size))
            print(f"DEBUG: gpkg_tile_matrix populated for zooms {int(global_minzoom)} to {int(global_maxzoom)}")

        elif db_type == 'mbtiles':
            meta = get_metadata(conn)
            meta['minzoom'] = str(int(global_minzoom))
            meta['maxzoom'] = str(int(global_maxzoom))
            meta['bounds'] = ','.join(f"{x:.6f}" for x in global_bounds_latlon)
            meta['format'] = meta.get('format', 'jpg')  # Assume jpg from user files
            conn.execute("DELETE FROM metadata;")
            for k, v in meta.items():
                conn.execute("INSERT INTO metadata (name, value) VALUES (?, ?)", (k, v))
            print("DEBUG: mbtiles metadata updated")

        conn.execute("COMMIT;")
        tile_count = conn.execute("SELECT COUNT(*) FROM tiles").fetchone()[0]
        print("\nMERGE SUCCESSFUL!")
        print(f"   Output: {output_file}")
        print(f"   Zoom: {int(global_minzoom)}–{int(global_maxzoom)}")
        print(f"   Bounds (lat/lon): {', '.join(f'{x:.6f}' for x in global_bounds_latlon)}")
        print(f"   Total tiles: {tile_count}")

        # Add overviews if flag set
        if add_overviews_flag:
            add_overviews(output_file)

    except Exception as e:
        conn.execute("ROLLBACK;")
        print(f"\nMERGE FAILED: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Merge multiple MBTiles/GeoPackage raster tile files.")
    parser.add_argument('inputs', nargs='+', help='Input .sqlite files (at least 2)')
    parser.add_argument('-o', '--output', required=True, help='Output merged .sqlite file')
    parser.add_argument('--force', action='store_true', help='Overwrite output if exists')
    parser.add_argument('--background_rgb', type=str, default='255,255,255',
                        help='RGB for background to skip (e.g., "255,255,255" for white, "0,0,0" for black)')
    parser.add_argument('--threshold', type=float, default=5.0,
                        help='Threshold for considering a tile as background (for JPEG artifacts)')
    parser.add_argument('--percentage', type=float, default=0.95,
                        help='Percentage of pixels that must match background (0.0-1.0)')
    parser.add_argument('--variance_threshold', type=float, default=10.0,
                        help='Variance threshold for considering a tile as background (low variance = uniform)')

    args = parser.parse_args()

    for f in args.inputs:
        if not os.path.isfile(f):
            print(f"File not found: {f}")
            sys.exit(1)

    merge_databases(
        args.inputs,
        args.output,
        overwrite=args.force,
        background_rgb=args.background_rgb,
        threshold=args.threshold,
        percentage=args.percentage,
        variance_threshold=args.variance_threshold
    )


if __name__ == '__main__':
    main()
