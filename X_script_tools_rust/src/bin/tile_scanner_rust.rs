use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use script_tools_rust::{read_placeholder_hashes, scan_tiles, TileScanOptions};

#[derive(Parser, Debug)]
#[command(name = "tile_scanner_rust")]
#[command(about = "Scan imagery tiles for bad candidates using Rust-first feature extraction")]
struct Args {
    #[arg(long)]
    root: PathBuf,

    #[arg(long, default_value = "tile_scan.json")]
    out: PathBuf,

    #[arg(long, default_value_t = 200)]
    tiny_bytes: u64,

    #[arg(long, default_value_t = 3.0)]
    flat_std_threshold: f32,

    #[arg(long, default_value_t = 30.0)]
    dark_mean_threshold: f32,

    #[arg(long, default_value_t = 10.0)]
    dark_std_threshold: f32,

    #[arg(long)]
    placeholder_hashes: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut options = TileScanOptions {
        tiny_bytes: args.tiny_bytes,
        flat_std_threshold: args.flat_std_threshold,
        dark_mean_threshold: args.dark_mean_threshold,
        dark_std_threshold: args.dark_std_threshold,
        placeholder_hashes: Default::default(),
    };
    if let Some(path) = args.placeholder_hashes.as_ref() {
        options.placeholder_hashes = read_placeholder_hashes(path)?;
    }
    let records = scan_tiles(&args.root, &options)?;
    std::fs::write(&args.out, serde_json::to_string_pretty(&records)?)?;
    println!("Wrote {} records to {}", records.len(), args.out.display());
    Ok(())
}
