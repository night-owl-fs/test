use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::{ConeJob, ProgressReporter, StepTimer};

use cone_to_heaven_rust::{
    cone_cells_for_job, expected_output_count, output_stem, tile_paths_for_cell,
    zoom_recipe_for_zoom,
};

#[derive(Parser, Debug)]
#[command(name = "cone_to_heaven_rust")]
#[command(about = "Build GeoTIFF mosaics from cone jobs and XYZ tile folders")]
struct Args {
    #[arg(long)]
    jobs: PathBuf,

    #[arg(long)]
    tiles_root: PathBuf,

    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long)]
    icao: Vec<String>,

    #[arg(long, default_value = "png")]
    ext: String,

    #[arg(long, default_value = "gdalbuildvrt")]
    gdalbuildvrt: String,

    #[arg(long, default_value = "gdal_translate")]
    gdal_translate: String,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    strict: bool,
}

fn run_cmd(program: &str, args: &[String], dry_run: bool) -> Result<()> {
    if dry_run {
        println!("DRY-RUN: {} {}", program, args.join(" "));
        return Ok(());
    }
    let status = Command::new(program)
        .args(args)
        .status()
        .with_context(|| format!("failed to spawn {program}"))?;
    if !status.success() {
        return Err(anyhow!("{program} failed with status {status}"));
    }
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timer = StepTimer::new(5, "5_step_geotiff_to_heaven", args.out_dir.clone());
    fs::create_dir_all(&args.out_dir)?;

    let jobs: Vec<ConeJob> = serde_json::from_str(
        &fs::read_to_string(&args.jobs)
            .with_context(|| format!("failed to read {}", args.jobs.display()))?,
    )
    .with_context(|| format!("failed to parse {}", args.jobs.display()))?;
    let total_jobs = jobs.len().max(1);
    let progress = ProgressReporter::new(5, "5_step_geotiff_to_heaven", total_jobs);
    progress.start(Some("Building GeoTIFF mosaics".to_string()));

    let icao_filter = args
        .icao
        .iter()
        .map(|x| x.trim().to_uppercase())
        .collect::<Vec<_>>();

    let mut built = 0usize;
    let skipped = 0usize;

    let mut errors = 0usize;
    for (index, job) in jobs.into_iter().enumerate() {
        if !icao_filter.is_empty() && !icao_filter.contains(&job.icao) {
            progress.update(
                index + 1,
                Some(total_jobs),
                errors,
                Some(format!("Skipped {} due to ICAO filter", job.icao)),
            );
            continue;
        }

        let recipe = zoom_recipe_for_zoom(job.out_z)
            .ok_or_else(|| anyhow!("No Step 5 zoom recipe defined for Z{}", job.out_z))?;
        let cells = cone_cells_for_job(&job)?;
        let expected = expected_output_count(recipe.pattern);
        let out_airport = args.out_dir.join(&job.icao).join(format!("Z{}", job.out_z));
        let mut produced_files = Vec::new();

        fs::create_dir_all(&out_airport)?;

        for cell in cells {
            let expected_tiles_in_cell = 1usize << ((job.out_z - job.base_z) * 2);
            let candidates = tile_paths_for_cell(&args.tiles_root, &job, &cell, &args.ext);
            let existing = candidates
                .into_iter()
                .filter(|p| p.exists())
                .collect::<Vec<_>>();

            if existing.is_empty() {
                let msg = format!(
                    "no source tiles found for {} Z{} cone r{} c{}",
                    job.icao, job.out_z, cell.row, cell.col
                );
                if args.strict {
                    return Err(anyhow!(msg));
                }
                eprintln!("[SKIP] {msg}");
                errors += 1;
                continue;
            }

            if existing.len() != expected_tiles_in_cell {
                let msg = format!(
                    "partial source tile set for {} Z{} cone r{} c{}: expected {}, got {}",
                    job.icao,
                    job.out_z,
                    cell.row,
                    cell.col,
                    expected_tiles_in_cell,
                    existing.len()
                );
                if args.strict {
                    return Err(anyhow!(msg));
                }
                eprintln!("[SKIP] {msg}");
                errors += 1;
                continue;
            }

            let stem = output_stem(&job, recipe, &cell);
            let list_path = out_airport.join(format!("{stem}.txt"));
            let vrt_path = out_airport.join(format!("{stem}.vrt"));
            let tif_path = out_airport.join(format!("{stem}.tif"));

            fs::write(
                &list_path,
                existing
                    .iter()
                    .map(|p| format!("{}\n", p.display()))
                    .collect::<String>(),
            )?;

            let vrt_args = vec![
                "-input_file_list".to_string(),
                list_path.display().to_string(),
                vrt_path.display().to_string(),
            ];
            run_cmd(&args.gdalbuildvrt, &vrt_args, args.dry_run)?;

            let tif_args = vec![
                "-of".to_string(),
                "GTiff".to_string(),
                "-a_srs".to_string(),
                "EPSG:3857".to_string(),
                "-co".to_string(),
                "TILED=YES".to_string(),
                "-co".to_string(),
                "COMPRESS=DEFLATE".to_string(),
                "-co".to_string(),
                "PREDICTOR=2".to_string(),
                vrt_path.display().to_string(),
                tif_path.display().to_string(),
            ];
            run_cmd(&args.gdal_translate, &tif_args, args.dry_run)?;

            println!(
                "[OK] {} Z{} ML{} cone r{} c{} -> {}",
                job.icao,
                job.out_z,
                recipe.mercator_level,
                cell.row,
                cell.col,
                tif_path.display()
            );
            produced_files.push(tif_path);
        }

        if produced_files.len() != expected {
            anyhow::bail!(
                "Step 5 output count mismatch for Z{}: expected {}, got {}",
                recipe.zoom,
                expected,
                produced_files.len()
            );
        }

        built += produced_files.len();
        progress.update(
            index + 1,
            Some(total_jobs),
            errors,
            Some(format!("Processed {} Z{}", job.icao, job.out_z)),
        );
    }

    println!("GeoTIFF stage complete");
    println!("built_files={built}");
    println!("skipped_jobs={skipped}");
    progress.finish(
        total_jobs,
        Some(total_jobs),
        errors,
        Some("GeoTIFF stage complete".to_string()),
    );
    let _ = timer.finish(
        Some(total_jobs),
        Some(built),
        Some(errors),
        format!(
            "Built {built} GeoTIFF outputs into {}",
            args.out_dir.display()
        ),
    )?;
    Ok(())
}
