use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::Parser;
use rusqlite::{params, Connection};

use merge_tiles_rust::{is_solid_background_tile, parse_rgb};

#[derive(Parser, Debug)]
#[command(name = "merge_tiles_rust")]
#[command(about = "Merge multiple tile SQLite/GeoPackage DBs (last input wins)")]
struct Args {
    #[arg(long, num_args = 1..)]
    inputs: Vec<PathBuf>,

    #[arg(long)]
    output: PathBuf,

    #[arg(long)]
    force: bool,

    #[arg(long)]
    skip_background: bool,

    #[arg(long, default_value = "0,0,0")]
    background_rgb: String,
}

fn discover_tile_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name",
    )?;
    let table_names = stmt
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    let mut out = Vec::new();
    for table in table_names {
        let pragma = format!("PRAGMA table_info({table})");
        let mut cols_stmt = conn.prepare(&pragma)?;
        let cols = cols_stmt
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        if ["zoom_level", "tile_column", "tile_row", "tile_data"]
            .iter()
            .all(|required| cols.iter().any(|c| c == required))
        {
            out.push(table);
        }
    }
    Ok(out)
}

fn ensure_table_exists(src_conn: &Connection, out_conn: &Connection, table: &str) -> Result<()> {
    let exists: bool = out_conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )?;
    if exists {
        return Ok(());
    }

    let create_sql: String = src_conn.query_row(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?1",
        [table],
        |row| row.get(0),
    )?;
    out_conn.execute_batch(&create_sql)?;
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.inputs.len() < 2 {
        return Err(anyhow!("pass at least two --inputs values"));
    }
    let bg = parse_rgb(&args.background_rgb).ok_or_else(|| {
        anyhow!("invalid --background-rgb format (expected r,g,b): {}", args.background_rgb)
    })?;

    if args.output.exists() && !args.force {
        return Err(anyhow!(
            "output already exists (use --force): {}",
            args.output.display()
        ));
    }
    if args.output.exists() && args.force {
        fs::remove_file(&args.output)?;
    }

    fs::copy(&args.inputs[0], &args.output)?;
    let out_conn = Connection::open(&args.output)?;

    let mut merged = 0usize;
    let mut skipped_bg = 0usize;

    for src_path in args.inputs.iter().skip(1) {
        let src_conn = Connection::open(src_path)?;
        let tile_tables = discover_tile_tables(&src_conn)?;
        for table in tile_tables {
            ensure_table_exists(&src_conn, &out_conn, &table)?;
            let query = format!(
                "SELECT zoom_level, tile_column, tile_row, tile_data FROM {table}"
            );
            let mut stmt = src_conn.prepare(&query)?;
            let mut rows = stmt.query([])?;

            let tx = out_conn.unchecked_transaction()?;
            let insert_sql = format!(
                "INSERT OR REPLACE INTO {table} (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)"
            );
            let mut insert_stmt = tx.prepare(&insert_sql)?;

            while let Some(row) = rows.next()? {
                let z: i64 = row.get(0)?;
                let x: i64 = row.get(1)?;
                let y: i64 = row.get(2)?;
                let tile_data: Vec<u8> = row.get(3)?;

                if args.skip_background && is_solid_background_tile(&tile_data, bg) {
                    skipped_bg += 1;
                    continue;
                }

                insert_stmt.execute(params![z, x, y, tile_data])?;
                merged += 1;
            }
            drop(insert_stmt);
            tx.commit()?;
            println!("[TABLE] merged tiles from {table} in {}", src_path.display());
        }
    }

    println!("Merge stage complete");
    println!("output={}", args.output.display());
    println!("tiles_merged={merged}");
    println!("tiles_skipped_background={skipped_bg}");
    Ok(())
}
