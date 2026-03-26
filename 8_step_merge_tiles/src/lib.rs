use anyhow::Result;
use image::RgbImage;
use rusqlite::{params, Connection, OptionalExtension};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TileDbType {
    Mbtiles,
    Geopackage,
    Generic,
}

#[derive(Debug, Clone, Copy)]
pub struct LatLonBounds {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct TableSummary {
    pub min_zoom: i64,
    pub max_zoom: i64,
    pub bounds: LatLonBounds,
}

pub fn parse_rgb(text: &str) -> Option<[u8; 3]> {
    let parts = text.split(',').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let r = parts[0].trim().parse::<u8>().ok()?;
    let g = parts[1].trim().parse::<u8>().ok()?;
    let b = parts[2].trim().parse::<u8>().ok()?;
    Some([r, g, b])
}

pub fn discover_tile_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for table in table_names {
        let pragma = format!("PRAGMA table_info({})", quote_ident(&table));
        let mut cols_stmt = conn.prepare(&pragma)?;
        let cols = cols_stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if table_has_tile_schema(&cols) {
            out.push(table);
        }
    }
    Ok(out)
}

pub fn detect_db_type(conn: &Connection) -> Result<TileDbType> {
    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    if names.iter().any(|t| t == "metadata") && names.iter().any(|t| t == "tiles") {
        return Ok(TileDbType::Mbtiles);
    }
    if names.iter().any(|t| t.starts_with("gpkg_"))
        && names.iter().any(|t| t == "tiles" || t.contains("tile"))
    {
        return Ok(TileDbType::Geopackage);
    }
    Ok(TileDbType::Generic)
}

pub fn ensure_table_schema(
    src_conn: &Connection,
    out_conn: &Connection,
    table: &str,
) -> Result<()> {
    if table_exists(out_conn, table)? {
        return Ok(());
    }

    let mut stmt = src_conn.prepare(
        "SELECT type, name, sql
         FROM sqlite_master
         WHERE tbl_name = ?1
           AND sql IS NOT NULL
           AND type IN ('table', 'index')
         ORDER BY CASE WHEN type = 'table' THEN 0 ELSE 1 END, name",
    )?;
    let entries = stmt
        .query_map([table], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    for (_kind, _name, sql) in entries {
        out_conn.execute_batch(&sql)?;
    }
    Ok(())
}

pub fn is_backgroundish_tile(
    bytes: &[u8],
    background: [u8; 3],
    threshold: f32,
    percentage: f32,
    variance_threshold: f32,
) -> bool {
    let Ok(img) = image::load_from_memory(bytes) else {
        return false;
    };
    let rgb = img.to_rgb8();
    image_is_backgroundish(&rgb, background, threshold, percentage, variance_threshold)
}

pub fn table_summary(conn: &Connection, table: &str) -> Result<Option<TableSummary>> {
    let table_name = quote_ident(table);
    let zoom_query = format!("SELECT MIN(zoom_level), MAX(zoom_level) FROM {table_name}");
    let (min_zoom, max_zoom): (Option<i64>, Option<i64>) =
        conn.query_row(&zoom_query, [], |row| Ok((row.get(0)?, row.get(1)?)))?;
    let (Some(min_zoom), Some(max_zoom)) = (min_zoom, max_zoom) else {
        return Ok(None);
    };

    let summary_query = format!(
        "SELECT MIN(tile_column), MAX(tile_column), MIN(tile_row), MAX(tile_row), zoom_level
         FROM {table_name}
         GROUP BY zoom_level"
    );
    let mut stmt = conn.prepare(&summary_query)?;
    let mut rows = stmt.query([])?;
    let mut bounds: Option<LatLonBounds> = None;

    while let Some(row) = rows.next()? {
        let min_col: i64 = row.get(0)?;
        let max_col: i64 = row.get(1)?;
        let min_row: i64 = row.get(2)?;
        let max_row: i64 = row.get(3)?;
        let zoom: i64 = row.get(4)?;
        let table_bounds = tile_range_to_bounds(min_col, max_col, min_row, max_row, zoom);
        bounds = Some(match bounds {
            Some(existing) => existing.union(table_bounds),
            None => table_bounds,
        });
    }

    Ok(bounds.map(|bounds| TableSummary {
        min_zoom,
        max_zoom,
        bounds,
    }))
}

pub fn merge_gpkg_metadata_for_table(
    src_conn: &Connection,
    out_conn: &Connection,
    table: &str,
) -> Result<()> {
    if !table_exists(src_conn, "gpkg_contents")? {
        return Ok(());
    }

    out_conn.execute(
        "INSERT OR IGNORE INTO gpkg_contents
         (table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id)
         SELECT table_name, data_type, identifier, description, last_change, min_x, min_y, max_x, max_y, srs_id
         FROM gpkg_contents
         WHERE table_name = ?1",
        [table],
    )?;

    if table_exists(src_conn, "gpkg_tile_matrix")? {
        let sql = "INSERT OR IGNORE INTO gpkg_tile_matrix
                   (table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size)
                   SELECT table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size
                   FROM gpkg_tile_matrix
                   WHERE table_name = ?1";
        out_conn.execute(sql, [table])?;
    }

    if table_exists(src_conn, "gpkg_tile_matrix_set")? {
        out_conn.execute(
            "INSERT OR IGNORE INTO gpkg_tile_matrix_set
             (table_name, srs_id, min_x, min_y, max_x, max_y)
             SELECT table_name, srs_id, min_x, min_y, max_x, max_y
             FROM gpkg_tile_matrix_set
             WHERE table_name = ?1",
            [table],
        )?;
    }

    Ok(())
}

pub fn finalize_geopackage_metadata(
    out_conn: &Connection,
    table: &str,
    summary: TableSummary,
) -> Result<()> {
    if !table_exists(out_conn, "gpkg_contents")? {
        return Ok(());
    }

    let (min_x, min_y) = lonlat_to_meters(summary.bounds.min_lon, summary.bounds.min_lat);
    let (max_x, max_y) = lonlat_to_meters(summary.bounds.max_lon, summary.bounds.max_lat);

    out_conn.execute(
        "UPDATE gpkg_contents
         SET min_x = ?1,
             min_y = ?2,
             max_x = ?3,
             max_y = ?4,
             last_change = strftime('%Y-%m-%dT%H:%M:%fZ', 'now')
         WHERE table_name = ?5",
        params![min_x, min_y, max_x, max_y, table],
    )?;

    if table_exists(out_conn, "gpkg_tile_matrix_set")? {
        out_conn.execute(
            "INSERT OR REPLACE INTO gpkg_tile_matrix_set
             (table_name, srs_id, min_x, min_y, max_x, max_y)
             VALUES (?1, 3857, ?2, ?3, ?4, ?5)",
            params![table, min_x, min_y, max_x, max_y],
        )?;
    }

    if table_exists(out_conn, "gpkg_tile_matrix")? {
        for zoom in summary.min_zoom..=summary.max_zoom {
            let matrix_size = 2f64.powi(zoom as i32);
            let pixel_size = 156543.03392804097 / 2f64.powi(zoom as i32);
            out_conn.execute(
                "INSERT OR IGNORE INTO gpkg_tile_matrix
                 (table_name, zoom_level, matrix_width, matrix_height, tile_width, tile_height, pixel_x_size, pixel_y_size)
                 VALUES (?1, ?2, ?3, ?4, 256, 256, ?5, ?5)",
                params![table, zoom, matrix_size as i64, matrix_size as i64, pixel_size],
            )?;
        }
    }

    Ok(())
}

pub fn finalize_mbtiles_metadata(out_conn: &Connection, summary: TableSummary) -> Result<()> {
    if !table_exists(out_conn, "metadata")? {
        return Ok(());
    }

    upsert_metadata_value(out_conn, "minzoom", &summary.min_zoom.to_string())?;
    upsert_metadata_value(out_conn, "maxzoom", &summary.max_zoom.to_string())?;
    upsert_metadata_value(
        out_conn,
        "bounds",
        &format!(
            "{:.6},{:.6},{:.6},{:.6}",
            summary.bounds.min_lon,
            summary.bounds.min_lat,
            summary.bounds.max_lon,
            summary.bounds.max_lat
        ),
    )?;
    let format = metadata_value(out_conn, "format")?.unwrap_or_else(|| "jpg".to_string());
    upsert_metadata_value(out_conn, "format", &format)?;
    Ok(())
}

pub fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

fn table_has_tile_schema(columns: &[String]) -> bool {
    ["zoom_level", "tile_column", "tile_row", "tile_data"]
        .iter()
        .all(|required| columns.iter().any(|c| c == required))
}

fn image_is_backgroundish(
    rgb: &RgbImage,
    background: [u8; 3],
    threshold: f32,
    percentage: f32,
    variance_threshold: f32,
) -> bool {
    let mut sum = 0.0f64;
    let mut sum_sq = 0.0f64;
    let mut close = 0usize;
    let total_values = (rgb.width() * rgb.height() * 3) as usize;
    let total_pixels = (rgb.width() * rgb.height()) as usize;

    if total_pixels == 0 {
        return false;
    }

    for pixel in rgb.pixels() {
        for channel in pixel.0 {
            let value = channel as f64;
            sum += value;
            sum_sq += value * value;
        }

        let dr = pixel[0] as f32 - background[0] as f32;
        let dg = pixel[1] as f32 - background[1] as f32;
        let db = pixel[2] as f32 - background[2] as f32;
        let distance = (dr * dr + dg * dg + db * db).sqrt();
        if distance < threshold {
            close += 1;
        }
    }

    let mean = sum / total_values as f64;
    let variance = ((sum_sq / total_values as f64) - (mean * mean)).max(0.0);
    if variance > variance_threshold as f64 {
        return false;
    }

    (close as f32 / total_pixels as f32) >= percentage
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let exists = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?1)",
        [table],
        |row| row.get::<_, i64>(0),
    )?;
    Ok(exists != 0)
}

fn metadata_value(conn: &Connection, key: &str) -> Result<Option<String>> {
    conn.query_row("SELECT value FROM metadata WHERE name = ?1", [key], |row| {
        row.get(0)
    })
    .optional()
    .map_err(Into::into)
}

fn upsert_metadata_value(conn: &Connection, key: &str, value: &str) -> Result<()> {
    conn.execute("DELETE FROM metadata WHERE name = ?1", [key])?;
    conn.execute(
        "INSERT INTO metadata (name, value) VALUES (?1, ?2)",
        params![key, value],
    )?;
    Ok(())
}

fn tile_range_to_bounds(
    min_col: i64,
    max_col: i64,
    min_row: i64,
    max_row: i64,
    zoom: i64,
) -> LatLonBounds {
    let n = 2f64.powi(zoom as i32);
    let lon1 = min_col as f64 / n * 360.0 - 180.0;
    let lon2 = (max_col + 1) as f64 / n * 360.0 - 180.0;
    let lat_north = 180.0 / std::f64::consts::PI
        * (2.0
            * (std::f64::consts::PI * (1.0 - 2.0 * min_row as f64 / n))
                .exp()
                .atan()
            - std::f64::consts::PI / 2.0);
    let lat_south = 180.0 / std::f64::consts::PI
        * (2.0
            * (std::f64::consts::PI * (1.0 - 2.0 * (max_row + 1) as f64 / n))
                .exp()
                .atan()
            - std::f64::consts::PI / 2.0);
    LatLonBounds {
        min_lon: lon1,
        min_lat: lat_south,
        max_lon: lon2,
        max_lat: lat_north,
    }
}

fn lonlat_to_meters(lon: f64, lat: f64) -> (f64, f64) {
    let x = lon * 20037508.34 / 180.0;
    let y = ((90.0 + lat) * std::f64::consts::PI / 360.0).tan().ln()
        / (std::f64::consts::PI / 180.0)
        * 20037508.34
        / 180.0;
    (x, y)
}

impl LatLonBounds {
    fn union(self, other: LatLonBounds) -> LatLonBounds {
        LatLonBounds {
            min_lon: self.min_lon.min(other.min_lon),
            min_lat: self.min_lat.min(other.min_lat),
            max_lon: self.max_lon.max(other.max_lon),
            max_lat: self.max_lat.max(other.max_lat),
        }
    }
}
