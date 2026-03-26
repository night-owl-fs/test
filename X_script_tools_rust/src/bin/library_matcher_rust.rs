use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use script_tools_rust::match_library;

#[derive(Parser, Debug)]
#[command(name = "library_matcher_rust")]
#[command(about = "Match a target image against the patch library using Rust-first features")]
struct Args {
    #[arg(long)]
    target: PathBuf,

    #[arg(long)]
    library: PathBuf,

    #[arg(long, default_value_t = 5)]
    topk: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let matches = match_library(&args.target, &args.library, args.topk)?;
    println!("{}", serde_json::to_string_pretty(&matches)?);
    Ok(())
}
