use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Result};
use clap::Parser;
use serde::Serialize;

use rust_imagery_tiler::{discover_geotiffs, render_command};

#[derive(Parser, Debug)]
#[command(name = "rust_imagery_tiler")]
#[command(about = "Automate Cesium imagery-tiler invocation from GeoTIFF inputs")]
struct Args {
    #[arg(long)]
    input_dir: PathBuf,

    #[arg(long)]
    output_db: PathBuf,

    #[arg(long, default_value = "imagery-tiler")]
    tiler_bin: String,

    #[arg(long)]
    working_directory: Option<PathBuf>,

    #[arg(long)]
    max_zoom_level_limit: Option<u8>,

    #[arg(long, default_value_t = true)]
    recursive: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    print_command_file: Option<PathBuf>,
}

#[derive(Serialize)]
struct TilerReport {
    geotiff_count: usize,
    output_db: String,
    command: String,
    dry_run: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let geotiffs = discover_geotiffs(&args.input_dir, args.recursive);
    if geotiffs.is_empty() {
        return Err(anyhow!(
            "no .tif/.tiff files found in {}",
            args.input_dir.display()
        ));
    }

    let mut cmd_parts = vec![args.tiler_bin.clone(), "-i".to_string()];
    cmd_parts.extend(geotiffs.iter().map(|p| p.display().to_string()));
    cmd_parts.extend([
        "-f".to_string(),
        "GEOPACKAGE".to_string(),
        "-o".to_string(),
        args.output_db.display().to_string(),
    ]);
    if let Some(max_zoom) = args.max_zoom_level_limit {
        cmd_parts.extend([
            "--max-zoom-level-limit".to_string(),
            max_zoom.to_string(),
        ]);
    }
    if let Some(ref working) = args.working_directory {
        cmd_parts.extend([
            "--working-directory".to_string(),
            working.display().to_string(),
        ]);
    }

    let command_text = render_command(&cmd_parts);
    if let Some(path) = args.print_command_file {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, format!("{command_text}\n"))?;
    }

    if args.dry_run {
        println!("DRY-RUN: {command_text}");
    } else {
        let mut process = Command::new(&args.tiler_bin);
        process.arg("-i");
        for tif in &geotiffs {
            process.arg(tif);
        }
        process.arg("-f").arg("GEOPACKAGE");
        process.arg("-o").arg(&args.output_db);
        if let Some(max_zoom) = args.max_zoom_level_limit {
            process.arg("--max-zoom-level-limit").arg(max_zoom.to_string());
        }
        if let Some(ref working) = args.working_directory {
            process.arg("--working-directory").arg(working);
        }
        let status = process.status()?;
        if !status.success() {
            return Err(anyhow!("imagery-tiler failed with status {status}"));
        }
    }

    let report = TilerReport {
        geotiff_count: geotiffs.len(),
        output_db: args.output_db.display().to_string(),
        command: command_text,
        dry_run: args.dry_run,
    };
    let report_path = args
        .output_db
        .parent()
        .unwrap_or_else(|| std::path::Path::new("."))
        .join("imagery_tiler_report.json");
    fs::write(&report_path, serde_json::to_string_pretty(&report)?)?;

    println!("Imagery tiler stage complete");
    println!("input_geotiffs={}", geotiffs.len());
    println!("output_db={}", args.output_db.display());
    println!("report={}", report_path.display());
    Ok(())
}
