use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use rusqlite::{params, Connection};

use rust_trim_job::{encode_jpeg, has_transparency, table_has_tile_schema};

#[derive(Parser, Debug)]
#[command(name = "rust_trim_job")]
#[command(about = "Optimize tile SQLite/GeoPackage DB by re-encoding tile_data to JPEG")]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    output: PathBuf,

    #[arg(long, default_value_t = 90)]
    quality: u8,

    #[arg(long)]
    min_zoom: Option<i64>,

    #[arg(long)]
    max_zoom: Option<i64>,

    #[arg(long)]
    skip_transparent: bool,

    #[arg(long)]
    force: bool,
}

fn find_tile_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;

    let mut tile_tables = Vec::new();
    for table in names {
        let pragma = format!("PRAGMA table_info({table})");
        let mut cols_stmt = conn.prepare(&pragma)?;
        let cols = cols_stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if table_has_tile_schema(&cols) {
            tile_tables.push(table);
        }
    }
    Ok(tile_tables)
}

fn zoom_where(min_zoom: Option<i64>, max_zoom: Option<i64>) -> String {
    match (min_zoom, max_zoom) {
        (Some(min), Some(max)) => format!("WHERE zoom_level BETWEEN {min} AND {max}"),
        (Some(min), None) => format!("WHERE zoom_level >= {min}"),
        (None, Some(max)) => format!("WHERE zoom_level <= {max}"),
        (None, None) => String::new(),
    }
}

fn main() -> Result<()> {
    let args = Args::parse();
    if !args.input.exists() {
        return Err(anyhow!("input does not exist: {}", args.input.display()));
    }
    if args.output.exists() && !args.force {
        return Err(anyhow!(
            "output already exists (use --force): {}",
            args.output.display()
        ));
    }
    if args.output.exists() && args.force {
        fs::remove_file(&args.output)?;
    }
    fs::copy(&args.input, &args.output).with_context(|| {
        format!(
            "failed to copy input {} -> {}",
            args.input.display(),
            args.output.display()
        )
    })?;

    let conn = Connection::open(&args.output)?;
    let tile_tables = find_tile_tables(&conn)?;
    if tile_tables.is_empty() {
        return Err(anyhow!(
            "no tile tables found in {}",
            args.output.display()
        ));
    }

    let mut total_seen = 0usize;
    let mut total_converted = 0usize;
    let mut total_skipped_alpha = 0usize;
    let mut total_decode_fail = 0usize;

    for table in tile_tables {
        let where_clause = zoom_where(args.min_zoom, args.max_zoom);
        let query =
            format!("SELECT rowid, zoom_level, tile_data FROM {table} {where_clause} ORDER BY rowid");
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query([])?;
        let mut updates = Vec::new();

        while let Some(row) = rows.next()? {
            total_seen += 1;
            let rowid: i64 = row.get(0)?;
            let blob: Vec<u8> = row.get(2)?;

            if args.skip_transparent && has_transparency(&blob) {
                total_skipped_alpha += 1;
                continue;
            }

            match encode_jpeg(&blob, args.quality) {
                Ok(jpeg) => {
                    updates.push((rowid, jpeg));
                }
                Err(_) => {
                    total_decode_fail += 1;
                }
            }
        }
        drop(rows);
        drop(stmt);

        let tx = conn.unchecked_transaction()?;
        {
            let mut update_stmt = tx.prepare(&format!("UPDATE {table} SET tile_data=?1 WHERE rowid=?2"))?;
            for (rowid, jpeg) in &updates {
                update_stmt.execute(params![jpeg, rowid])?;
            }
        }
        tx.commit()?;
        total_converted += updates.len();
        println!(
            "[TABLE] {table}: converted={} skipped_alpha={} decode_fail={}",
            updates.len(),
            total_skipped_alpha,
            total_decode_fail
        );
    }

    println!("Trim stage complete");
    println!("input={}", args.input.display());
    println!("output={}", args.output.display());
    println!("tiles_seen={total_seen}");
    println!("tiles_converted={total_converted}");
    println!("tiles_skipped_transparent={total_skipped_alpha}");
    println!("tiles_decode_fail={total_decode_fail}");
    Ok(())
}
