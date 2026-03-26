use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use script_tools_rust::generate_patterns;

#[derive(Parser, Debug)]
#[command(name = "pattern_generator_rust")]
#[command(about = "Generate procedural patch-library tiles with Rust-first patterns")]
struct Args {
    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long, default_value_t = 128)]
    size: u32,

    #[arg(
        long,
        num_args = 1..,
        default_values_t = vec![
            "water".to_string(),
            "dark_water".to_string(),
            "light_water".to_string(),
            "grass".to_string(),
            "dark_grass".to_string(),
            "light_grass".to_string(),
            "brown_mountain".to_string(),
        ]
    )]
    presets: Vec<String>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let generated = generate_patterns(&args.out_dir, args.size, &args.presets)?;
    let rendered = generated
        .iter()
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();
    println!("Generated: {:?}", rendered);
    Ok(())
}
