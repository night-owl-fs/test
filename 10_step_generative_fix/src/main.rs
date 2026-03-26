use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use pipeline_core::{ProgressReporter, StepTimer};

use generative_fix_rust::{build_super_repair_plan, SuperFillConfig};

#[derive(Parser, Debug)]
#[command(name = "generative_fix_rust")]
#[command(about = "Super generative tile fill planner + optional writer")]
struct Args {
    #[arg(long)]
    tiles_root: PathBuf,

    #[arg(long)]
    airport: Vec<String>,

    #[arg(long)]
    zoom: Vec<u32>,

    #[arg(long, default_value_t = 50_000)]
    max_missing: usize,

    #[arg(long, default_value = "repair_plan.json")]
    out_plan: PathBuf,

    #[arg(long, default_value_t = false)]
    apply: bool,

    #[arg(long, default_value_t = false)]
    skip_bad_tiles: bool,

    #[arg(long, default_value_t = 3_500)]
    tiny_bytes: u64,

    #[arg(long, default_value_t = 5.0)]
    dark_mean_threshold: f32,

    #[arg(long, default_value_t = 3.0)]
    dark_std_threshold: f32,

    #[arg(long, default_value_t = 0.9)]
    flat_std_threshold: f32,

    #[arg(long, default_value_t = 75.0)]
    flat_mean_ceiling: f32,

    #[arg(long)]
    placeholder_hash: Vec<String>,

    #[arg(long, default_value_t = 10)]
    neighbor_radius: u32,

    #[arg(long, default_value_t = 24)]
    large_gap_radius: u32,

    #[arg(long, default_value_t = false)]
    disable_context_tuning: bool,

    #[arg(long, default_value_t = 1.12)]
    water_blue_gain: f32,

    #[arg(long, default_value_t = 0.92)]
    water_green_gain: f32,

    #[arg(long, default_value_t = 1.12)]
    greenery_green_gain: f32,

    #[arg(long, default_value_t = 0.92)]
    greenery_blue_gain: f32,

    #[arg(long, default_value_t = 1.04)]
    global_saturation: f32,

    #[arg(long, default_value_t = 1.03)]
    global_contrast: f32,
    #[arg(long)]
    patch_library: Option<String>,
    #[arg(long, default_value_t = false)]
    enable_patch_matching: bool,
    #[arg(long, default_value_t = 1)]
    patch_topk: u32,

    #[arg(long, default_value_t = false)]
    no_worldfiles: bool,

    #[arg(long, default_value_t = false)]
    seam_aware_blend: bool,

    #[arg(long, default_value_t = 16)]
    seam_feather_px: u32,

    #[arg(long, default_value_t = 1.0)]
    seam_neighbor_weight: f32,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timing_root = args
        .out_plan
        .parent()
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    let timer = StepTimer::new(10, "10_step_generative_fix", timing_root);
    let progress = ProgressReporter::new(10, "10_step_generative_fix", 1);
    progress.start(Some("Planning generative fixes".to_string()));

    let mut config = SuperFillConfig {
        repair_bad_tiles: !args.skip_bad_tiles,
        tiny_bytes: args.tiny_bytes,
        dark_mean_threshold: args.dark_mean_threshold,
        dark_std_threshold: args.dark_std_threshold,
        flat_std_threshold: args.flat_std_threshold,
        flat_mean_ceiling: args.flat_mean_ceiling,
        placeholder_hashes: SuperFillConfig::default().placeholder_hashes,
        neighbor_radius: args.neighbor_radius,
        large_gap_radius: args.large_gap_radius,
        enable_context_tuning: !args.disable_context_tuning,
        water_blue_gain: args.water_blue_gain,
        water_green_gain: args.water_green_gain,
        greenery_green_gain: args.greenery_green_gain,
        greenery_blue_gain: args.greenery_blue_gain,
        global_saturation: args.global_saturation,
        global_contrast: args.global_contrast,
        write_worldfiles: !args.no_worldfiles,
        seam_aware_blend: args.seam_aware_blend,
        seam_feather_px: args.seam_feather_px,
        seam_neighbor_weight: args.seam_neighbor_weight,
        patch_library: args.patch_library.clone(),
        enable_patch_matching: args.enable_patch_matching,
        patch_topk: args.patch_topk,
    };

    if !args.placeholder_hash.is_empty() {
        config.placeholder_hashes.extend(args.placeholder_hash);
        config
            .placeholder_hashes
            .iter_mut()
            .for_each(|h| *h = h.trim().to_ascii_lowercase());
        config.placeholder_hashes.sort();
        config.placeholder_hashes.dedup();
    }

    let plan = build_super_repair_plan(
        &args.tiles_root,
        &args.zoom,
        &args.airport,
        args.max_missing,
        args.apply,
        config,
    )?;

    if let Some(parent) = args.out_plan.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }
    fs::write(&args.out_plan, serde_json::to_string_pretty(&plan)?)?;

    println!("Super generative fill planning complete");
    println!("targets={}", plan.missing_count);
    println!("generated={}", plan.generated_count);
    println!("written={}", plan.written_count);
    println!("unresolved={}", plan.unresolved_count);
    println!("apply_mode={}", args.apply);
    println!("plan_file={}", args.out_plan.display());
    progress.finish(
        1,
        Some(1),
        plan.unresolved_count,
        Some("Generative fix planning complete".to_string()),
    );
    let _ = timer.finish(
        Some(plan.missing_count),
        Some(plan.generated_count),
        Some(plan.unresolved_count),
        format!("Generated repair plan at {}", args.out_plan.display()),
    )?;
    Ok(())
}
