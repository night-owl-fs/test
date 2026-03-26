use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use script_tools_rust::{
    default_patch_catalog_output, default_patch_library_path, generate_patch_catalog,
};

#[derive(Parser, Debug)]
#[command(name = "generate_patch_catalog_rust")]
#[command(about = "Generate patch_library/catalog.json with Rust-first metadata scanning")]
struct Args {
    #[arg(long, default_value_os_t = default_patch_library_path())]
    library: PathBuf,

    #[arg(long)]
    out: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let out = args
        .out
        .unwrap_or_else(|| default_patch_catalog_output(&args.library));
    generate_patch_catalog(&args.library, &out)?;
    println!("Wrote {}", out.display());
    Ok(())
}
