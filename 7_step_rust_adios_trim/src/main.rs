use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::{ProgressReporter, StepTimer};
use rusqlite::{params, Connection};

use rust_trim_job::{encode_jpeg, parse_rgb, table_has_tile_schema};

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
    skip_fully_transparent: bool,

    #[arg(long, visible_alias = "skip-transparent")]
    skip_any_alpha: bool,

    #[arg(long, default_value = "0,0,0")]
    background: String,

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

fn table_tile_count(conn: &Connection, table: &str, where_clause: &str) -> Result<usize> {
    let query = format!("SELECT COUNT(*) FROM {table} {where_clause}");
    let count: i64 = conn.query_row(&query, [], |row| row.get(0))?;
    Ok(count.max(0) as usize)
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timing_root = args
        .output
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let timer = StepTimer::new(7, "7_step_rust_adios_trim", timing_root);
    if !(1..=100).contains(&args.quality) {
        return Err(anyhow!("quality must be in the range 1..=100"));
    }
    let background = parse_rgb(&args.background).ok_or_else(|| {
        anyhow!(
            "invalid --background format (expected r,g,b): {}",
            args.background
        )
    })?;
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
        return Err(anyhow!("no tile tables found in {}", args.output.display()));
    }
    let total_tables = tile_tables.len().max(1);
    let progress = ProgressReporter::new(7, "7_step_rust_adios_trim", total_tables);
    progress.start(Some("Trimming imagery database".to_string()));

    let mut total_seen = 0usize;
    let mut total_converted = 0usize;
    let mut total_skipped_alpha = 0usize;
    let mut total_decode_fail = 0usize;

    for (index, table) in tile_tables.into_iter().enumerate() {
        let where_clause = zoom_where(args.min_zoom, args.max_zoom);
        let table_total = table_tile_count(&conn, &table, &where_clause)?;
        let query = format!(
            "SELECT rowid, zoom_level, tile_data FROM {table} {where_clause} ORDER BY rowid"
        );
        let mut stmt = conn.prepare(&query)?;
        let mut rows = stmt.query([])?;
        let mut updates = Vec::new();
        let table_start = Instant::now();
        let mut last_report = Instant::now();
        let mut table_seen = 0usize;
        let mut table_converted = 0usize;
        let mut table_skipped = 0usize;
        let mut table_decode_fail = 0usize;

        while let Some(row) = rows.next()? {
            total_seen += 1;
            table_seen += 1;
            let rowid: i64 = row.get(0)?;
            let blob: Vec<u8> = row.get(2)?;

            match encode_jpeg(
                &blob,
                args.quality,
                background,
                args.skip_fully_transparent,
                args.skip_any_alpha,
            ) {
                Ok(Some(jpeg)) => {
                    updates.push((rowid, jpeg));
                    table_converted += 1;
                }
                Ok(None) => {
                    total_skipped_alpha += 1;
                    table_skipped += 1;
                }
                Err(_) => {
                    total_decode_fail += 1;
                    table_decode_fail += 1;
                }
            }

            if last_report.elapsed() >= Duration::from_secs(1) {
                let elapsed = table_start.elapsed().as_secs_f64();
                let rate = if elapsed > 0.0 {
                    table_seen as f64 / elapsed
                } else {
                    0.0
                };
                let remaining = table_total.saturating_sub(table_seen);
                let eta_minutes = if rate > 0.0 {
                    remaining as f64 / rate / 60.0
                } else {
                    0.0
                };
                println!(
                    "  {table}: {table_seen}/{table_total} converted={table_converted} skipped={table_skipped} decode_fail={table_decode_fail} rate={rate:.1} tiles/s eta={eta_minutes:.1}m"
                );
                last_report = Instant::now();
            }
        }
        drop(rows);
        drop(stmt);

        let tx = conn.unchecked_transaction()?;
        {
            let mut update_stmt =
                tx.prepare(&format!("UPDATE {table} SET tile_data=?1 WHERE rowid=?2"))?;
            for (rowid, jpeg) in &updates {
                update_stmt.execute(params![jpeg, rowid])?;
            }
        }
        tx.commit()?;
        total_converted += table_converted;
        let elapsed = table_start.elapsed().as_secs_f64();
        let rate = if elapsed > 0.0 {
            table_seen as f64 / elapsed
        } else {
            0.0
        };
        println!(
            "[TABLE] {table}: seen={table_seen} converted={table_converted} skipped_alpha={table_skipped} decode_fail={table_decode_fail} elapsed={elapsed:.1}s rate={rate:.1} tiles/s",
        );
        progress.update(
            index + 1,
            Some(total_tables),
            total_decode_fail,
            Some(format!("Processed table {table}")),
        );
    }

    println!("Trim stage complete");
    println!("input={}", args.input.display());
    println!("output={}", args.output.display());
    println!("tiles_seen={total_seen}");
    println!("tiles_converted={total_converted}");
    println!("tiles_skipped_transparent={total_skipped_alpha}");
    println!("tiles_decode_fail={total_decode_fail}");
    println!(
        "background={},{},{}",
        background[0], background[1], background[2]
    );
    println!("skip_fully_transparent={}", args.skip_fully_transparent);
    println!("skip_any_alpha={}", args.skip_any_alpha);
    progress.finish(
        total_tables,
        Some(total_tables),
        total_decode_fail,
        Some("Trim stage complete".to_string()),
    );
    let _ = timer.finish(
        Some(total_seen),
        Some(total_converted),
        Some(total_decode_fail),
        format!("Trimmed imagery database into {}", args.output.display()),
    )?;
    Ok(())
}
