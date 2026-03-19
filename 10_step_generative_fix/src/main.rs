use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;

use generative_fix_rust::discover_xyz_tiles;

#[derive(Parser, Debug)]
#[command(name = "generative_fix_rust")]
#[command(about = "Experimental missing-tile detector and repair-plan generator")]
struct Args {
    #[arg(long)]
    tiles_root: PathBuf,

    #[arg(long)]
    zoom: Vec<u32>,

    #[arg(long, default_value_t = 50_000)]
    max_missing: usize,

    #[arg(long, default_value = "repair_plan.json")]
    out_plan: PathBuf,
}

#[derive(Debug, Serialize)]
struct MissingTile {
    z: u32,
    x: u32,
    y: u32,
    parent_hint: Option<(u32, u32, u32)>,
}

#[derive(Debug, Serialize)]
struct RepairPlan {
    missing_count: usize,
    missing: Vec<MissingTile>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let discovered = discover_xyz_tiles(&args.tiles_root);
    let requested = if args.zoom.is_empty() {
        discovered.keys().copied().collect::<Vec<_>>()
    } else {
        args.zoom.clone()
    };

    let mut missing = Vec::new();
    for z in requested {
        let Some(coords) = discovered.get(&z) else {
            continue;
        };
        let min_x = coords.iter().map(|(x, _)| *x).min().unwrap_or(0);
        let max_x = coords.iter().map(|(x, _)| *x).max().unwrap_or(0);
        let min_y = coords.iter().map(|(_, y)| *y).min().unwrap_or(0);
        let max_y = coords.iter().map(|(_, y)| *y).max().unwrap_or(0);

        for x in min_x..=max_x {
            for y in min_y..=max_y {
                if coords.contains(&(x, y)) {
                    continue;
                }
                let parent_hint = if z > 0 {
                    Some((z - 1, x / 2, y / 2))
                } else {
                    None
                };
                missing.push(MissingTile { z, x, y, parent_hint });
                if missing.len() >= args.max_missing {
                    break;
                }
            }
            if missing.len() >= args.max_missing {
                break;
            }
        }
    }

    let plan = RepairPlan {
        missing_count: missing.len(),
        missing,
    };
    if let Some(parent) = args.out_plan.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(&args.out_plan, serde_json::to_string_pretty(&plan)?)?;

    println!("Generative-fix planning complete");
    println!("missing_tiles_detected={}", plan.missing_count);
    println!("plan_file={}", args.out_plan.display());
    Ok(())
}
