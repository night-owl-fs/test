use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::{
    build_airport_cone_to_heaven_report, build_download_manifest, render_cone_spec_text, ConeJob,
    DownloadManifest, DownloadManifestItem,
};
use rayon::prelude::*;

#[derive(Parser, Debug)]
#[command(
    name = "downloader_template",
    about = "Reusable multi-airport Cone-to-Heaven downloader template"
)]
struct Args {
    /// Mode A: input cone jobs JSON path.
    #[arg(long)]
    jobs_json: Option<PathBuf>,

    /// Mode B: airport DB path. Use with --icao.
    #[arg(long)]
    db: Option<PathBuf>,

    /// ICAO list for Mode B (e.g. --icao KSAN KTPA KJFK)
    #[arg(long, num_args = 1..)]
    icao: Vec<String>,

    /// Tile URL template (must contain {z} {x} {y}).
    #[arg(long, default_value = "https://tiles.example.invalid/{z}/{x}/{y}.png")]
    url_template: String,

    /// Output root for everything (manifest, jobs, tile folders, logs, scripts).
    #[arg(short, long, default_value = "TILES_TEMPLATE")]
    output: PathBuf,

    /// Optional explicit manifest output path. Defaults to <output>/download_manifest.json.
    #[arg(long)]
    out_manifest: Option<PathBuf>,

    /// Optional explicit jobs output path for Mode B. Defaults to <output>/airport_cones.json.
    #[arg(long)]
    out_jobs: Option<PathBuf>,

    /// Write K###_spec.txt files when building jobs from DB.
    #[arg(long, default_value_t = true)]
    write_specs_txt: bool,

    /// Max parallel workers.
    #[arg(long, default_value_t = 16)]
    workers: usize,

    /// Retry count per tile request.
    #[arg(long, default_value_t = 3)]
    retries: usize,

    /// HTTP timeout in seconds.
    #[arg(long, default_value_t = 20)]
    timeout_secs: u64,

    /// Skip existing tiles if present.
    #[arg(long, default_value_t = true)]
    resume: bool,

    /// Print requests without downloading bytes.
    #[arg(long)]
    dry_run: bool,

    /// Optional airport name for precision filenames when using jobs_json mode.
    #[arg(long)]
    airport_label: Option<String>,

    /// Approximate center latitude for precision calculations.
    #[arg(long, default_value_t = 0.0)]
    center_lat: f64,

    /// Optional measurement CRS for helper script generation.
    #[arg(long)]
    truth_crs: Option<String>,

    /// Optional measurement resolution (m/px) for helper script generation.
    #[arg(long)]
    truth_res: Option<f64>,
}

#[derive(Debug, Clone)]
struct ZoomStats {
    zoom: u32,
    min_x: u32,
    max_x: u32,
    min_y: u32,
    max_y: u32,
    tile_count: usize,
}

fn load_jobs_from_json(path: &Path) -> Result<Vec<ConeJob>> {
    let text = fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let jobs: Vec<ConeJob> =
        serde_json::from_str(&text).with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(jobs)
}

fn save_jobs(path: &Path, jobs: &[ConeJob]) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(path, serde_json::to_string_pretty(jobs)?)?;
    Ok(())
}

fn resolve_jobs(args: &Args) -> Result<(Vec<ConeJob>, Option<Vec<(String, String, f64)>>)> {
    if let Some(path) = &args.jobs_json {
        let jobs = load_jobs_from_json(path)?;
        return Ok((jobs, None));
    }

    let db = args
        .db
        .as_ref()
        .ok_or_else(|| anyhow!("Either --jobs-json or (--db + --icao) is required"))?;
    if args.icao.is_empty() {
        return Err(anyhow!("--icao is required with --db"));
    }

    let report = build_airport_cone_to_heaven_report(db, &args.icao)?;
    if !report.airports_missing.is_empty() {
        return Err(anyhow!("ICAOs missing in DB: {:?}", report.airports_missing));
    }

    if args.write_specs_txt {
        let specs_dir = args.output.join("specs");
        fs::create_dir_all(&specs_dir)?;
        for airport in &report.airports {
            let airport_jobs = report
                .jobs
                .iter()
                .filter(|j| j.icao == airport.icao)
                .cloned()
                .collect::<Vec<_>>();
            let txt = render_cone_spec_text(airport, &airport_jobs);
            let path = specs_dir.join(format!("{}_spec.txt", airport.icao));
            fs::write(path, txt)?;
        }
    }

    let airport_meta = report
        .airports
        .iter()
        .map(|a| (a.icao.clone(), a.name.clone(), a.lat))
        .collect::<Vec<_>>();
    Ok((report.jobs, Some(airport_meta)))
}

fn compute_zoom_stats(items: &[DownloadManifestItem]) -> Vec<ZoomStats> {
    let mut map: BTreeMap<u32, ZoomStats> = BTreeMap::new();
    for item in items {
        map.entry(item.z)
            .and_modify(|s| {
                s.min_x = s.min_x.min(item.x);
                s.max_x = s.max_x.max(item.x);
                s.min_y = s.min_y.min(item.y);
                s.max_y = s.max_y.max(item.y);
                s.tile_count += 1;
            })
            .or_insert_with(|| ZoomStats {
                zoom: item.z,
                min_x: item.x,
                max_x: item.x,
                min_y: item.y,
                max_y: item.y,
                tile_count: 1,
            });
    }
    map.into_values().collect()
}

fn pixel_size_meters(zoom: u32, center_lat_deg: f64) -> f64 {
    let earth_radius_m = 6_378_137.0_f64;
    let earth_circumference_m = 2.0 * std::f64::consts::PI * earth_radius_m;
    let denom = 256.0 * (1u64 << zoom) as f64;
    let meters_per_pixel_equator = earth_circumference_m / denom;
    meters_per_pixel_equator * center_lat_deg.to_radians().cos()
}

fn write_precision_log(output: &Path, airport_label: &str, center_lat: f64, stats: &[ZoomStats]) -> Result<()> {
    let csv_path = output.join(format!("precision_{}.csv", airport_label));
    let mut file = File::create(&csv_path)
        .with_context(|| format!("Failed to create {}", csv_path.display()))?;
    writeln!(
        file,
        "airport,zoom,center_lat_deg,pixel_size_m,width_px,height_px,width_m,height_m,tile_count"
    )?;

    for s in stats {
        let px_size = pixel_size_meters(s.zoom, center_lat);
        let tiles_x = (s.max_x - s.min_x + 1) as u64;
        let tiles_y = (s.max_y - s.min_y + 1) as u64;
        let width_px = tiles_x * 256;
        let height_px = tiles_y * 256;
        let width_m = width_px as f64 * px_size;
        let height_m = height_px as f64 * px_size;

        writeln!(
            file,
            "{airport},{zoom},{lat},{px:.6},{wpx},{hpx},{wm:.3},{hm:.3},{count}",
            airport = airport_label,
            zoom = s.zoom,
            lat = center_lat,
            px = px_size,
            wpx = width_px,
            hpx = height_px,
            wm = width_m,
            hm = height_m,
            count = s.tile_count
        )?;
    }

    println!("Precision CSV written: {}", csv_path.display());
    Ok(())
}

fn write_truth_scripts(output: &Path, airport_label: &str, truth_crs: &str, truth_res: f64, stats: &[ZoomStats]) -> Result<()> {
    let sh_path = output.join(format!("truth_geotiffs_{}.sh", airport_label));
    let ps1_path = output.join(format!("truth_geotiffs_{}.ps1", airport_label));

    let mut sh = File::create(&sh_path)?;
    writeln!(sh, "#!/usr/bin/env bash")?;
    writeln!(sh, "set -euo pipefail")?;
    writeln!(sh, "cd \"$(dirname \"$0\")\"")?;
    writeln!(sh, "")?;

    let mut ps1 = File::create(&ps1_path)?;
    writeln!(ps1, "$ErrorActionPreference = 'Stop'")?;
    writeln!(ps1, "Set-Location (Split-Path -Parent $MyInvocation.MyCommand.Path)")?;
    writeln!(ps1, "")?;

    for s in stats {
        let vrt_name = format!("{}_Z{}_3857.vrt", airport_label, s.zoom);
        let tif_name = format!(
            "{}_Z{}_{}m_{}.tif",
            airport_label,
            s.zoom,
            truth_res,
            truth_crs.replace(':', "")
        );
        writeln!(sh, "gdalbuildvrt \"{vrt_name}\" \"{z}/*.png\"", z = s.zoom)?;
        writeln!(
            sh,
            "gdalwarp -t_srs {crs} -tr {res} {res} -r bilinear -of COG \"{vrt}\" \"{tif}\"",
            crs = truth_crs,
            res = truth_res,
            vrt = vrt_name,
            tif = tif_name
        )?;
        writeln!(sh)?;

        writeln!(ps1, "gdalbuildvrt \"{vrt_name}\" \"{z}/*.png\"", z = s.zoom)?;
        writeln!(
            ps1,
            "gdalwarp -t_srs {crs} -tr {res} {res} -r bilinear -of COG \"{vrt}\" \"{tif}\"",
            crs = truth_crs,
            res = truth_res,
            vrt = vrt_name,
            tif = tif_name
        )?;
        writeln!(ps1)?;
    }

    println!(
        "Truth scripts written: {} and {}",
        sh_path.display(),
        ps1_path.display()
    );
    Ok(())
}

fn download_items(args: &Args, manifest: &DownloadManifest) -> Result<()> {
    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()
        .ok();

    let client = Arc::new(
        reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(args.timeout_secs))
            .build()
            .context("Failed to build HTTP client")?,
    );

    let attempted = AtomicUsize::new(0);
    let downloaded = AtomicUsize::new(0);
    let skipped = AtomicUsize::new(0);
    let failed = AtomicUsize::new(0);

    manifest.items.par_iter().for_each(|item| {
        attempted.fetch_add(1, Ordering::Relaxed);
        let file_path = args.output.join(&item.relative_path);

        if args.resume && file_path.exists() {
            skipped.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if args.dry_run {
            downloaded.fetch_add(1, Ordering::Relaxed);
            return;
        }

        if let Some(parent) = file_path.parent() {
            if fs::create_dir_all(parent).is_err() {
                failed.fetch_add(1, Ordering::Relaxed);
                return;
            }
        }

        let mut ok = false;
        for _ in 0..=args.retries {
            let resp = client.get(&item.url).send();
            let Ok(resp) = resp else {
                continue;
            };
            if !resp.status().is_success() {
                continue;
            }
            let Ok(bytes) = resp.bytes() else {
                continue;
            };
            if fs::write(&file_path, &bytes).is_ok() {
                ok = true;
                break;
            }
        }

        if ok {
            downloaded.fetch_add(1, Ordering::Relaxed);
        } else {
            failed.fetch_add(1, Ordering::Relaxed);
            eprintln!("FAILED: z={} x={} y={} url={}", item.z, item.x, item.y, item.url);
        }
    });

    println!("Download summary");
    println!("attempted={}", attempted.load(Ordering::Relaxed));
    println!("downloaded={}", downloaded.load(Ordering::Relaxed));
    println!("skipped_existing={}", skipped.load(Ordering::Relaxed));
    println!("failed={}", failed.load(Ordering::Relaxed));
    Ok(())
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.workers == 0 {
        return Err(anyhow!("--workers must be at least 1"));
    }
    fs::create_dir_all(&args.output)?;

    let (jobs, airport_meta) = resolve_jobs(&args)?;
    let jobs_out = args
        .out_jobs
        .clone()
        .unwrap_or_else(|| args.output.join("airport_cones.json"));
    save_jobs(&jobs_out, &jobs)?;
    println!("Jobs JSON written: {}", jobs_out.display());

    let manifest = build_download_manifest(&jobs, &args.url_template);
    let manifest_path = args
        .out_manifest
        .clone()
        .unwrap_or_else(|| args.output.join("download_manifest.json"));
    if let Some(parent) = manifest_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&manifest_path, serde_json::to_string_pretty(&manifest)?)?;
    println!("Manifest written: {}", manifest_path.display());
    println!(
        "Manifest summary: airports={} tiles={}",
        manifest.airport_count, manifest.tile_count
    );

    let stats = compute_zoom_stats(&manifest.items);
    let airport_label = args.airport_label.clone().unwrap_or_else(|| {
        if let Some(meta) = &airport_meta {
            if meta.len() == 1 {
                return meta[0].0.clone();
            }
            return "MULTI".to_string();
        }
        "TEMPLATE".to_string()
    });
    let center_lat = if args.center_lat != 0.0 {
        args.center_lat
    } else if let Some(meta) = &airport_meta {
        if meta.len() == 1 {
            meta[0].2
        } else {
            0.0
        }
    } else {
        0.0
    };
    write_precision_log(&args.output, &airport_label, center_lat, &stats)?;

    if let (Some(crs), Some(res)) = (args.truth_crs.as_deref(), args.truth_res) {
        write_truth_scripts(&args.output, &airport_label, crs, res, &stats)?;
    }

    download_items(&args, &manifest)?;
    println!("Downloader template run complete.");
    Ok(())
}
