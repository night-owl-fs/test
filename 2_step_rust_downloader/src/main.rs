use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::{
    build_airport_cone_to_heaven_report, build_download_manifest, render_cone_spec_text, ConeJob,
    DownloadManifest, ProgressReporter, StepTimer,
};
use rayon::prelude::*;
use reqwest::blocking::Client;
use step_rust_downloader::{normalize_download_manifest_to_png, write_png_file};

#[derive(Parser, Debug)]
#[command(name = "step_rust_downloader")]
#[command(about = "Generate cone-to-heaven jobs, manifest, and optional tile downloads")]
struct Args {
    /// Existing jobs JSON file (mode A)
    #[arg(long)]
    spec: Option<PathBuf>,

    /// Airport DB file for auto-cone mode (mode B)
    #[arg(long)]
    db: Option<PathBuf>,

    /// ICAO list for auto-cone mode (mode B)
    #[arg(long, num_args = 1..)]
    icao: Vec<String>,

    /// Output manifest file (always written)
    #[arg(long)]
    out_manifest: PathBuf,

    /// Optional output jobs JSON path (used in mode B; default beside manifest)
    #[arg(long)]
    out_jobs: Option<PathBuf>,

    /// Optional output folder for K###_spec.txt files (mode B)
    #[arg(long)]
    out_specs_dir: Option<PathBuf>,

    /// Tile URL template
    #[arg(long, default_value = "https://tiles.example.invalid/{z}/{x}/{y}.png")]
    url_template: String,

    /// Download tile bytes into this folder if set
    #[arg(long)]
    download_root: Option<PathBuf>,

    /// Parallel worker count for optional download
    #[arg(long, default_value_t = 8)]
    workers: usize,

    /// Retry attempts per tile during optional download
    #[arg(long, default_value_t = 3)]
    retries: usize,

    /// HTTP timeout for optional download
    #[arg(long, default_value_t = 30)]
    timeout_secs: u64,

    /// Skip already existing output tile files
    #[arg(long, default_value_t = true)]
    resume: bool,

    /// Print-only mode for download stage
    #[arg(long)]
    dry_run: bool,
}

fn read_jobs_from_spec(path: &Path) -> Result<Vec<ConeJob>> {
    let text =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let jobs: Vec<ConeJob> = serde_json::from_str(&text)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(jobs)
}

fn prepare_jobs(args: &Args) -> Result<Vec<ConeJob>> {
    if let Some(spec_path) = &args.spec {
        return read_jobs_from_spec(spec_path);
    }

    let db = args
        .db
        .as_ref()
        .ok_or_else(|| anyhow!("Either --spec or (--db + --icao) is required"))?;
    if args.icao.is_empty() {
        return Err(anyhow!("--icao is required when --db is used"));
    }

    let report = build_airport_cone_to_heaven_report(db, &args.icao)?;
    if !report.airports_missing.is_empty() {
        eprintln!("[WARN] Missing ICAOs: {:?}", report.airports_missing);
    }

    let jobs_path = args
        .out_jobs
        .clone()
        .unwrap_or_else(|| args.out_manifest.with_file_name("airport_cones.json"));
    if let Some(parent) = jobs_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&jobs_path, serde_json::to_string_pretty(&report.jobs)?)?;
    println!("Wrote jobs JSON: {}", jobs_path.display());

    let specs_dir = args.out_specs_dir.clone().unwrap_or_else(|| {
        args.out_manifest
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .to_path_buf()
    });
    fs::create_dir_all(&specs_dir)?;
    for airport in &report.airports {
        let airport_jobs = report
            .jobs
            .iter()
            .filter(|j| j.icao == airport.icao)
            .cloned()
            .collect::<Vec<_>>();
        let text = render_cone_spec_text(airport, &airport_jobs);
        let spec_path = specs_dir.join(format!("{}_spec.txt", airport.icao));
        fs::write(&spec_path, text)?;
        println!("Wrote text cone spec: {}", spec_path.display());
    }

    Ok(report.jobs)
}

fn write_manifest(args: &Args, jobs: &[ConeJob]) -> Result<DownloadManifest> {
    let manifest = build_download_manifest(jobs, &args.url_template);
    if let Some(parent) = args.out_manifest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&args.out_manifest, serde_json::to_string_pretty(&manifest)?)?;
    println!("Wrote download manifest: {}", args.out_manifest.display());
    println!(
        "Manifest summary: airports={} tiles={}",
        manifest.airport_count, manifest.tile_count
    );
    Ok(manifest)
}

fn maybe_download(args: &Args, manifest: &DownloadManifest) -> Result<()> {
    let total_items = manifest.items.len().max(1);
    let progress = ProgressReporter::new(2, "2_step_rust_downloader", total_items);
    progress.start(Some("Preparing download stage".to_string()));

    let Some(download_root) = &args.download_root else {
        progress.finish(
            manifest.items.len(),
            Some(total_items),
            0,
            Some("Manifest ready; download stage skipped".to_string()),
        );
        return Ok(());
    };

    fs::create_dir_all(download_root)?;
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()
        .ok();

    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout_secs))
        .build()?;

    let attempted = AtomicUsize::new(0);
    let downloaded = AtomicUsize::new(0);
    let skipped_existing = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);
    let processed = AtomicUsize::new(0);

    manifest.items.par_iter().for_each(|item| {
        let target = download_root.join(&item.relative_path);
        attempted.fetch_add(1, Ordering::Relaxed);

        if args.resume && target.exists() {
            skipped_existing.fetch_add(1, Ordering::Relaxed);
            let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == manifest.items.len() || done % 250 == 0 {
                progress.update(
                    done,
                    Some(total_items),
                    failed.load(Ordering::Relaxed),
                    Some("Downloading tiles".to_string()),
                );
            }
            return;
        }

        if args.dry_run {
            downloaded.fetch_add(1, Ordering::Relaxed);
            let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == manifest.items.len() || done % 250 == 0 {
                progress.update(
                    done,
                    Some(total_items),
                    failed.load(Ordering::Relaxed),
                    Some("Dry-run download progress".to_string()),
                );
            }
            return;
        }

        if let Some(parent) = target.parent() {
            if fs::create_dir_all(parent).is_err() {
                failed.fetch_add(1, Ordering::Relaxed);
                let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == manifest.items.len() || done % 250 == 0 {
                    progress.update(
                        done,
                        Some(total_items),
                        failed.load(Ordering::Relaxed),
                        Some("Downloading tiles".to_string()),
                    );
                }
                return;
            }
        }

        let mut success = false;
        for _ in 0..=args.retries {
            let response = client.get(&item.url).send();
            let Ok(response) = response else {
                continue;
            };
            if !response.status().is_success() {
                continue;
            }
            let bytes = response.bytes();
            let Ok(bytes) = bytes else {
                continue;
            };
            if write_png_file(&target, &bytes).is_ok() {
                success = true;
                break;
            }
        }
        if success {
            downloaded.fetch_add(1, Ordering::Relaxed);
        } else {
            failed.fetch_add(1, Ordering::Relaxed);
        }

        let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
        if done == manifest.items.len() || done % 250 == 0 {
            progress.update(
                done,
                Some(total_items),
                failed.load(Ordering::Relaxed),
                Some("Downloading tiles".to_string()),
            );
        }
    });

    println!("Download stage summary");
    println!("attempted={}", attempted.load(Ordering::Relaxed));
    println!("downloaded={}", downloaded.load(Ordering::Relaxed));
    println!(
        "skipped_existing={}",
        skipped_existing.load(Ordering::Relaxed)
    );
    println!("failed={}", failed.load(Ordering::Relaxed));

    if !args.dry_run {
        progress.update(
            manifest.items.len(),
            Some(total_items),
            failed.load(Ordering::Relaxed),
            Some("Converting downloaded files to PNG".to_string()),
        );

        let png_report = normalize_download_manifest_to_png(download_root, manifest);
        println!("PNG conversion summary");
        println!("converted={}", png_report.converted);
        println!("missing={}", png_report.missing);
        println!("failed={}", png_report.failed);

        if !png_report.sample_errors.is_empty() {
            for error in &png_report.sample_errors {
                eprintln!("PNG conversion issue: {error}");
            }
        }

        if png_report.missing > 0 || png_report.failed > 0 {
            progress.finish(
                manifest.items.len(),
                Some(total_items),
                failed.load(Ordering::Relaxed) + png_report.missing + png_report.failed,
                Some("Download stage finished with PNG conversion errors".to_string()),
            );
            return Err(anyhow!(
                "PNG conversion incomplete: converted={} missing={} failed={}",
                png_report.converted,
                png_report.missing,
                png_report.failed
            ));
        }

        println!(
            "[PNG-CONVERSION] All {} tile files were converted and verified as PNG in {}",
            png_report.converted,
            download_root.display()
        );
    }

    progress.finish(
        manifest.items.len(),
        Some(total_items),
        failed.load(Ordering::Relaxed),
        Some("Download stage complete".to_string()),
    );

    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timing_root = args
        .download_root
        .clone()
        .or_else(|| args.out_manifest.parent().map(PathBuf::from))
        .unwrap_or_else(|| PathBuf::from("."));
    let timer = StepTimer::new(2, "2_step_rust_downloader", timing_root);
    let jobs = prepare_jobs(&args)?;
    let manifest = write_manifest(&args, &jobs)?;
    maybe_download(&args, &manifest)?;
    let _ = timer.finish(
        Some(jobs.len()),
        Some(manifest.tile_count),
        Some(0),
        format!(
            "Manifest built for {} airports and {} tiles",
            manifest.airport_count, manifest.tile_count
        ),
    )?;
    Ok(())
}
