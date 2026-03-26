use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Result};
use clap::Parser;
use pipeline_core::{ProgressReporter, StepTimer};
use rusqlite::{params, Connection};

use merge_tiles_rust::{
    detect_db_type, discover_tile_tables, ensure_table_schema, finalize_geopackage_metadata,
    finalize_mbtiles_metadata, is_backgroundish_tile, merge_gpkg_metadata_for_table, parse_rgb,
    quote_ident, table_summary, TileDbType,
};

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

    #[arg(long, default_value_t = 5.0)]
    threshold: f32,

    #[arg(long, default_value_t = 0.95)]
    percentage: f32,

    #[arg(long, default_value_t = 10.0)]
    variance_threshold: f32,

    #[arg(long, default_value_t = false)]
    add_overviews: bool,
}

fn cleanup_background_tiles(
    conn: &Connection,
    table: &str,
    background: [u8; 3],
    threshold: f32,
    percentage: f32,
    variance_threshold: f32,
) -> Result<usize> {
    let table_name = quote_ident(table);
    let query = format!("SELECT rowid, tile_data FROM {table_name}");
    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;
    let mut to_delete = Vec::new();

    while let Some(row) = rows.next()? {
        let rowid: i64 = row.get(0)?;
        let blob: Vec<u8> = row.get(1)?;
        if is_backgroundish_tile(&blob, background, threshold, percentage, variance_threshold) {
            to_delete.push(rowid);
        }
    }
    drop(rows);
    drop(stmt);

    if to_delete.is_empty() {
        return Ok(0);
    }

    let tx = conn.unchecked_transaction()?;
    let delete_sql = format!("DELETE FROM {table_name} WHERE rowid = ?1");
    {
        let mut delete_stmt = tx.prepare(&delete_sql)?;
        for rowid in &to_delete {
            delete_stmt.execute(params![rowid])?;
        }
    }
    tx.commit()?;
    Ok(to_delete.len())
}

fn merge_source_table(
    src_conn: &Connection,
    out_conn: &Connection,
    table: &str,
    skip_background: bool,
    background: [u8; 3],
    threshold: f32,
    percentage: f32,
    variance_threshold: f32,
) -> Result<(usize, usize)> {
    let table_name = quote_ident(table);
    let query = format!("SELECT zoom_level, tile_column, tile_row, tile_data FROM {table_name}");
    let mut stmt = src_conn.prepare(&query)?;
    let mut rows = stmt.query([])?;

    let tx = out_conn.unchecked_transaction()?;
    let insert_sql = format!(
        "INSERT OR REPLACE INTO {table_name} (zoom_level, tile_column, tile_row, tile_data) VALUES (?1, ?2, ?3, ?4)"
    );
    let mut insert_stmt = tx.prepare(&insert_sql)?;

    let mut inserted = 0usize;
    let mut skipped = 0usize;

    while let Some(row) = rows.next()? {
        let z: i64 = row.get(0)?;
        let x: i64 = row.get(1)?;
        let y: i64 = row.get(2)?;
        let tile_data: Vec<u8> = row.get(3)?;

        if skip_background
            && is_backgroundish_tile(
                &tile_data,
                background,
                threshold,
                percentage,
                variance_threshold,
            )
        {
            skipped += 1;
            continue;
        }

        insert_stmt.execute(params![z, x, y, tile_data])?;
        inserted += 1;
    }
    drop(insert_stmt);
    tx.commit()?;
    Ok((inserted, skipped))
}

fn maybe_add_overviews(output: &PathBuf, db_type: TileDbType, tables: &[String]) -> Result<()> {
    if db_type != TileDbType::Geopackage {
        println!("Skipping overviews: only GeoPackage outputs are supported");
        return Ok(());
    }

    for table in tables {
        let status = Command::new("gdaladdo")
            .arg("-r")
            .arg("average")
            .arg("-ro")
            .arg(format!("GPKG:{}:{table}", output.display()))
            .args(["2", "4", "8", "16"])
            .status()?;
        if !status.success() {
            return Err(anyhow!(
                "gdaladdo failed for table {table} with status {status}"
            ));
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timing_root = args
        .output
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let timer = StepTimer::new(8, "8_step_merge_tiles", timing_root);
    if args.inputs.len() < 2 {
        return Err(anyhow!("pass at least two --inputs values"));
    }
    let background = parse_rgb(&args.background_rgb).ok_or_else(|| {
        anyhow!(
            "invalid --background-rgb format (expected r,g,b): {}",
            args.background_rgb
        )
    })?;
    if !(0.0..=1.0).contains(&args.percentage) {
        return Err(anyhow!("--percentage must be between 0.0 and 1.0"));
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

    fs::copy(&args.inputs[0], &args.output)?;
    let out_conn = Connection::open(&args.output)?;
    let db_type = detect_db_type(&out_conn)?;
    let total_sources = args.inputs.iter().skip(1).count().max(1);
    let progress = ProgressReporter::new(8, "8_step_merge_tiles", total_sources);
    progress.start(Some("Merging tile databases".to_string()));

    let mut merged = 0usize;
    let mut skipped_bg = 0usize;
    println!("Detected format: {:?}", db_type);

    if args.skip_background {
        for table in discover_tile_tables(&out_conn)? {
            let deleted = cleanup_background_tiles(
                &out_conn,
                &table,
                background,
                args.threshold,
                args.percentage,
                args.variance_threshold,
            )?;
            if deleted > 0 {
                skipped_bg += deleted;
                println!("[PRIMARY] removed {deleted} background tiles from {table}");
            }
        }
    }

    for (index, src_path) in args.inputs.iter().skip(1).enumerate() {
        let src_conn = Connection::open(src_path)?;
        let tile_tables = discover_tile_tables(&src_conn)?;
        for table in tile_tables {
            ensure_table_schema(&src_conn, &out_conn, &table)?;
            if db_type == TileDbType::Geopackage {
                merge_gpkg_metadata_for_table(&src_conn, &out_conn, &table)?;
            }
            let (inserted, skipped) = merge_source_table(
                &src_conn,
                &out_conn,
                &table,
                args.skip_background,
                background,
                args.threshold,
                args.percentage,
                args.variance_threshold,
            )?;
            merged += inserted;
            skipped_bg += skipped;
            println!(
                "[TABLE] merged {inserted} tiles from {table} in {} (skipped_background={skipped})",
                src_path.display(),
            );
        }
        progress.update(
            index + 1,
            Some(total_sources),
            0,
            Some(format!("Merged {}", src_path.display())),
        );
    }

    let final_tables = discover_tile_tables(&out_conn)?;
    match db_type {
        TileDbType::Geopackage => {
            for table in &final_tables {
                if let Some(summary) = table_summary(&out_conn, table)? {
                    finalize_geopackage_metadata(&out_conn, table, summary)?;
                    println!(
                        "[META] {table}: zoom={}..{} bounds={:.6},{:.6},{:.6},{:.6}",
                        summary.min_zoom,
                        summary.max_zoom,
                        summary.bounds.min_lon,
                        summary.bounds.min_lat,
                        summary.bounds.max_lon,
                        summary.bounds.max_lat
                    );
                }
            }
        }
        TileDbType::Mbtiles => {
            if let Some(summary) = table_summary(&out_conn, "tiles")? {
                finalize_mbtiles_metadata(&out_conn, summary)?;
                println!(
                    "[META] tiles: zoom={}..{} bounds={:.6},{:.6},{:.6},{:.6}",
                    summary.min_zoom,
                    summary.max_zoom,
                    summary.bounds.min_lon,
                    summary.bounds.min_lat,
                    summary.bounds.max_lon,
                    summary.bounds.max_lat
                );
            }
        }
        TileDbType::Generic => {}
    }

    if args.add_overviews {
        maybe_add_overviews(&args.output, db_type, &final_tables)?;
    }

    println!("Merge stage complete");
    println!("output={}", args.output.display());
    println!("tiles_merged={merged}");
    println!("tiles_skipped_background={skipped_bg}");
    progress.finish(
        total_sources,
        Some(total_sources),
        0,
        Some("Merge stage complete".to_string()),
    );
    let _ = timer.finish(
        Some(args.inputs.len()),
        Some(1),
        Some(0),
        format!(
            "Merged {} inputs into {}",
            args.inputs.len(),
            args.output.display()
        ),
    )?;
    Ok(())
}
