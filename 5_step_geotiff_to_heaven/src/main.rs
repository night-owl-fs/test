use std::fs;
use std::path::PathBuf;
use std::process::Command;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::ConeJob;

use cone_to_heaven_rust::tile_path_from_job;

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

fn job_stem(job: &ConeJob) -> String {
    format!(
        "{}_Z{}_fromZ{}_baseX{}_baseY{}_grid{}",
        job.icao, job.out_z, job.base_z, job.base_x, job.base_y, job.grid
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.out_dir)?;

    let jobs: Vec<ConeJob> = serde_json::from_str(
        &fs::read_to_string(&args.jobs)
            .with_context(|| format!("failed to read {}", args.jobs.display()))?,
    )
    .with_context(|| format!("failed to parse {}", args.jobs.display()))?;

    let icao_filter = args
        .icao
        .iter()
        .map(|x| x.trim().to_uppercase())
        .collect::<Vec<_>>();

    let mut built = 0usize;
    let mut skipped = 0usize;

    for job in jobs {
        if !icao_filter.is_empty() && !icao_filter.contains(&job.icao) {
            continue;
        }

        let candidates = tile_path_from_job(&args.tiles_root, &job, &args.ext);
        let existing = candidates
            .into_iter()
            .filter(|p| p.exists())
            .collect::<Vec<_>>();

        if existing.is_empty() {
            skipped += 1;
            let msg = format!(
                "no source tiles found for {} (out_z={})",
                job.icao, job.out_z
            );
            if args.strict {
                return Err(anyhow!(msg));
            }
            eprintln!("[SKIP] {msg}");
            continue;
        }

        let stem = job_stem(&job);
        let out_airport = args.out_dir.join(&job.icao).join(format!("Z{}", job.out_z));
        fs::create_dir_all(&out_airport)?;
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
            "[OK] {} tiles -> {}",
            existing.len(),
            tif_path.display()
        );
        built += 1;
    }

    println!("GeoTIFF stage complete");
    println!("built_jobs={built}");
    println!("skipped_jobs={skipped}");
    Ok(())
}
