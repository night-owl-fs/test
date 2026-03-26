use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use generative_fix_rust::write_worldfiles_png;
use image::{imageops, DynamicImage};
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use reqwest::blocking::Client;
use serde_json::Value;
use tempfile::NamedTempFile;

const WEBM_HALF: f64 = 20037508.342789244;
const WEBM_WORLD: f64 = WEBM_HALF * 2.0;
const TILE_SIZE: u32 = 256;

#[derive(Parser, Debug)]
#[command(name = "step10_legacy_tools_rust")]
#[command(about = "Rust replacement for step 10 legacy maintenance tools")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    DownloadWindow {
        #[arg(long)]
        url_template: String,
        #[arg(long, default_value = ".")]
        dest_root: PathBuf,
        #[arg(long, action = clap::ArgAction::Append)]
        window: Vec<String>,
        #[arg(long, default_value_t = false)]
        include_pgw: bool,
        #[arg(long, default_value_t = 20)]
        workers: usize,
        #[arg(long, default_value_t = 4)]
        retries: u32,
        #[arg(long, default_value_t = 30)]
        timeout: u64,
        #[arg(long, default_value_t = false)]
        overwrite_existing: bool,
        #[arg(long, default_value_t = false)]
        dry_run: bool,
        #[arg(long)]
        failure_report: Option<PathBuf>,
    },
    FlattenXy {
        #[arg(long)]
        src: PathBuf,
        #[arg(long)]
        dst: PathBuf,
        #[arg(long, default_value = ".png")]
        ext: String,
    },
    PaintWindow {
        #[arg(long)]
        image: PathBuf,
        #[arg(long)]
        tile_dir: PathBuf,
        #[arg(long)]
        zoom: u32,
        #[arg(long)]
        x0: u32,
        #[arg(long)]
        y0: u32,
        #[arg(long, default_value_t = 32)]
        tiles: u32,
        #[arg(long, default_value_t = false)]
        no_worldfiles: bool,
    },
    RebuildCones {
        #[arg(long, default_value = ".")]
        root: PathBuf,
        #[arg(long, action = clap::ArgAction::Append)]
        file: Vec<PathBuf>,
        #[arg(long)]
        file_list: Option<PathBuf>,
        #[arg(long, default_value_t = false)]
        best_size: bool,
        #[arg(long, action = clap::ArgAction::Append)]
        force_src_zoom: Vec<u32>,
    },
}

#[derive(Clone)]
struct DownloadJob {
    url: String,
    dst: PathBuf,
    min_bytes: u64,
}

#[derive(Debug, Clone)]
struct WindowSpec {
    airport: String,
    zoom: u32,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
}

#[derive(Debug, Clone, Copy)]
struct TargetMeta {
    out_z: u32,
    base_z: u32,
    bx: u32,
    by: u32,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::DownloadWindow {
            url_template,
            dest_root,
            window,
            include_pgw,
            workers,
            retries,
            timeout,
            overwrite_existing,
            dry_run,
            failure_report,
        } => cmd_download_window(
            &url_template,
            &dest_root,
            &window,
            include_pgw,
            workers,
            retries,
            timeout,
            overwrite_existing,
            dry_run,
            failure_report.as_deref(),
        ),
        Commands::FlattenXy { src, dst, ext } => cmd_flatten_xy(&src, &dst, &ext),
        Commands::PaintWindow {
            image,
            tile_dir,
            zoom,
            x0,
            y0,
            tiles,
            no_worldfiles,
        } => cmd_paint_window(&image, &tile_dir, zoom, x0, y0, tiles, no_worldfiles),
        Commands::RebuildCones {
            root,
            file,
            file_list,
            best_size,
            force_src_zoom,
        } => cmd_rebuild_cones(
            &root,
            &file,
            file_list.as_deref(),
            best_size,
            &force_src_zoom,
        ),
    }
}

fn cmd_download_window(
    url_template: &str,
    dest_root: &Path,
    windows: &[String],
    include_pgw: bool,
    workers: usize,
    retries: u32,
    timeout_secs: u64,
    overwrite_existing: bool,
    dry_run: bool,
    failure_report: Option<&Path>,
) -> Result<()> {
    if windows.is_empty() {
        bail!("pass at least one --window AIRPORT:ZOOM:XMIN-XMAX:YMIN-YMAX");
    }

    let mut jobs = Vec::new();
    for raw in windows {
        let spec = parse_window_spec(raw)?;
        for x in spec.x_min..=spec.x_max {
            for y in spec.y_min..=spec.y_max {
                let png_path = dest_root
                    .join(&spec.airport)
                    .join(spec.zoom.to_string())
                    .join(format!("{x}_{y}.png"));
                jobs.push(DownloadJob {
                    url: build_url(url_template, &spec.airport, spec.zoom, x, y, "png"),
                    dst: png_path,
                    min_bytes: 100,
                });
                if include_pgw {
                    let pgw_path = dest_root
                        .join(&spec.airport)
                        .join(spec.zoom.to_string())
                        .join(format!("{x}_{y}.pgw"));
                    jobs.push(DownloadJob {
                        url: build_url(url_template, &spec.airport, spec.zoom, x, y, "pgw"),
                        dst: pgw_path,
                        min_bytes: 20,
                    });
                }
            }
        }
    }

    println!("planned_downloads={}", jobs.len());
    if dry_run {
        for job in jobs.iter().take(40) {
            println!("DRY {} -> {}", job.url, job.dst.display());
        }
        if jobs.len() > 40 {
            println!("... and {} more", jobs.len() - 40);
        }
        return Ok(());
    }

    let client = Client::builder()
        .user_agent("step10-legacy-tools-rust/1.0")
        .timeout(Duration::from_secs(timeout_secs))
        .build()?;
    let failures = Arc::new(Mutex::new(Vec::new()));
    let ok_count = AtomicUsize::new(0);
    let done_count = AtomicUsize::new(0);
    let total_jobs = jobs.len();

    let pool = ThreadPoolBuilder::new()
        .num_threads(workers.max(1))
        .build()?;
    pool.install(|| {
        jobs.into_par_iter().for_each(|job| {
            let result = download_job(&client, &job, retries, overwrite_existing)
                .map_err(|err| format!("{} -> {}: {err}", job.url, job.dst.display()));
            match result {
                Ok(()) => {
                    ok_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(message) => {
                    if let Ok(mut locked) = failures.lock() {
                        locked.push(message);
                    }
                }
            }
            let done = done_count.fetch_add(1, Ordering::Relaxed) + 1;
            if done % 250 == 0 || done == total_jobs {
                let failures_len = failures.lock().map(|v| v.len()).unwrap_or_default();
                println!(
                    "progress={done}/{total_jobs} ok={} fail={failures_len}",
                    ok_count.load(Ordering::Relaxed)
                );
            }
        });
    });

    let failures = failures.lock().map(|v| v.clone()).unwrap_or_default();
    if let Some(report) = failure_report {
        if !failures.is_empty() {
            if let Some(parent) = report.parent() {
                fs::create_dir_all(parent)?;
            }
            fs::write(report, failures.join("\n") + "\n")?;
            println!("failure_report={}", report.display());
        }
    }

    println!("downloaded_ok={}", ok_count.load(Ordering::Relaxed));
    println!("downloaded_fail={}", failures.len());
    if failures.is_empty() {
        Ok(())
    } else {
        bail!("one or more downloads failed")
    }
}

fn cmd_flatten_xy(src: &Path, dst: &Path, ext: &str) -> Result<()> {
    if !src.exists() {
        bail!("missing source: {}", src.display());
    }

    fs::create_dir_all(dst)?;
    let normalized_ext = ext.to_ascii_lowercase();
    let mut copied = 0usize;
    let mut skipped = 0usize;
    let mut bad = 0usize;

    for entry in fs::read_dir(src)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(file_ext) = path.extension().and_then(|v| v.to_str()) else {
            continue;
        };
        let wanted = normalized_ext.trim_start_matches('.');
        if !file_ext.eq_ignore_ascii_case(wanted) {
            continue;
        }
        let Some((x, y)) = parse_xy_name(&path) else {
            bad += 1;
            continue;
        };
        let out_dir = dst.join(x.to_string());
        fs::create_dir_all(&out_dir)?;
        let out_file = out_dir.join(format!("{y}.{}", wanted));
        if out_file.exists() && out_file.metadata()?.len() > 0 {
            skipped += 1;
            continue;
        }
        fs::copy(&path, &out_file)?;
        copied += 1;
    }

    println!("copied={copied}");
    println!("skipped={skipped}");
    println!("bad_names={bad}");
    Ok(())
}

fn cmd_paint_window(
    image_path: &Path,
    tile_dir: &Path,
    zoom: u32,
    x0: u32,
    y0: u32,
    tiles: u32,
    no_worldfiles: bool,
) -> Result<()> {
    let image = image::open(image_path)
        .with_context(|| format!("failed to open fill image {}", image_path.display()))?;
    let canvas = image.to_rgb8();
    let resized = imageops::resize(
        &canvas,
        tiles * TILE_SIZE,
        tiles * TILE_SIZE,
        imageops::FilterType::Lanczos3,
    );

    let mut replaced = 0usize;
    for row in 0..tiles {
        for col in 0..tiles {
            let x = x0 + col;
            let y = y0 + row;
            let crop = imageops::crop_imm(
                &resized,
                col * TILE_SIZE,
                row * TILE_SIZE,
                TILE_SIZE,
                TILE_SIZE,
            )
            .to_image();
            let out_png = tile_dir.join(format!("{x}_{y}.png"));
            fs::create_dir_all(tile_dir)?;
            DynamicImage::ImageRgb8(crop).save_with_format(&out_png, image::ImageFormat::Png)?;
            if !no_worldfiles {
                write_worldfiles_png(&out_png, zoom, x, y)?;
            }
            replaced += 1;
        }
    }

    println!("replaced_tiles={replaced}");
    println!("image={}", image_path.display());
    println!(
        "window=x:{x0}-{}, y:{y0}-{}",
        x0 + tiles - 1,
        y0 + tiles - 1
    );
    Ok(())
}

fn cmd_rebuild_cones(
    root: &Path,
    files: &[PathBuf],
    file_list: Option<&Path>,
    best_size: bool,
    forced_src_zooms: &[u32],
) -> Result<()> {
    let root = root.canonicalize().unwrap_or_else(|_| root.to_path_buf());
    let mut targets = files.to_vec();
    if let Some(list_path) = file_list {
        let content = fs::read_to_string(list_path)?;
        for line in content.lines() {
            let value = line.trim();
            if !value.is_empty() {
                targets.push(PathBuf::from(value));
            }
        }
    }
    if targets.is_empty() {
        bail!("no targets provided; use --file and/or --file-list");
    }

    let airport_dirs = collect_airport_dirs(&root)?;
    let mut rebuilt = 0usize;
    let mut failed = 0usize;

    for target in targets {
        let tif = if target.is_absolute() {
            target
        } else {
            root.join(target)
        };
        let meta = parse_target_metadata(
            tif.file_name()
                .and_then(|v| v.to_str())
                .ok_or_else(|| anyhow!("bad target filename: {}", tif.display()))?,
        )?;

        let airport = tif
            .parent()
            .and_then(|p| p.parent())
            .and_then(|p| p.file_name())
            .and_then(|v| v.to_str())
            .ok_or_else(|| anyhow!("failed to resolve airport from {}", tif.display()))?;
        let airport_dir = resolve_airport_dir(&root, airport);

        let mut candidate_zooms = vec![
            meta.out_z + 1,
            meta.out_z + 2,
            meta.out_z.saturating_sub(1),
            meta.out_z + 3,
            meta.out_z.saturating_sub(2),
            meta.out_z + 4,
            meta.out_z.saturating_sub(3),
        ];
        candidate_zooms.retain(|z| (13..=18).contains(z) && *z != meta.out_z);
        if !forced_src_zooms.is_empty() {
            candidate_zooms = forced_src_zooms
                .iter()
                .copied()
                .filter(|z| (13..=18).contains(z) && *z != meta.out_z)
                .collect();
        }

        if best_size {
            let mut best_path: Option<PathBuf> = None;
            let mut best_score = f64::MIN;
            let mut best_src_zoom = None;

            for src_zoom in &candidate_zooms {
                let candidate_path = tif.with_file_name(format!(
                    "{}.src{src_zoom}.tif",
                    tif.file_stem().and_then(|v| v.to_str()).unwrap_or("target")
                ));
                let ok = build_from_zoom(
                    &airport_dir,
                    &airport_dirs,
                    &candidate_path,
                    meta,
                    *src_zoom,
                )?;
                if ok && candidate_path.exists() {
                    let score = quality_score_gdal(&candidate_path)?;
                    if score > best_score {
                        best_score = score;
                        best_src_zoom = Some(*src_zoom);
                        best_path = Some(candidate_path.clone());
                    }
                } else if candidate_path.exists() {
                    let _ = fs::remove_file(&candidate_path);
                }
                let _ = fs::remove_file(candidate_path.with_extension("vrt"));
            }

            if let Some(best_path) = best_path {
                if let Some(parent) = tif.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::rename(&best_path, &tif)?;
                println!(
                    "[OK] {} from Z{} score={best_score:.2}",
                    tif.display(),
                    best_src_zoom.unwrap_or(0)
                );
                rebuilt += 1;
            } else {
                println!("[FAIL] {}", tif.display());
                failed += 1;
            }
        } else {
            let mut ok_any = false;
            for src_zoom in &candidate_zooms {
                let tmp = tif.with_file_name(format!(
                    "{}.tmp.tif",
                    tif.file_stem().and_then(|v| v.to_str()).unwrap_or("target")
                ));
                let ok = build_from_zoom(&airport_dir, &airport_dirs, &tmp, meta, *src_zoom)?;
                if ok {
                    if let Some(parent) = tif.parent() {
                        fs::create_dir_all(parent)?;
                    }
                    fs::rename(&tmp, &tif)?;
                    println!("[OK] {} from Z{}", tif.display(), src_zoom);
                    rebuilt += 1;
                    ok_any = true;
                    break;
                }
                let _ = fs::remove_file(&tmp);
                let _ = fs::remove_file(tmp.with_extension("vrt"));
            }
            if !ok_any {
                println!("[FAIL] {}", tif.display());
                failed += 1;
            }
        }
    }

    println!("rebuilt={rebuilt}");
    println!("failed={failed}");
    if failed == 0 {
        Ok(())
    } else {
        bail!("one or more cone rebuilds failed")
    }
}

fn parse_window_spec(raw: &str) -> Result<WindowSpec> {
    let parts = raw.split(':').collect::<Vec<_>>();
    if parts.len() != 4 {
        bail!("bad window spec '{raw}' (expected AIRPORT:ZOOM:XMIN-XMAX:YMIN-YMAX)");
    }

    let airport = parts[0].trim().to_ascii_uppercase();
    let zoom = parts[1].trim().parse::<u32>()?;
    let (x_min, x_max) = parse_range(parts[2])?;
    let (y_min, y_max) = parse_range(parts[3])?;
    Ok(WindowSpec {
        airport,
        zoom,
        x_min,
        x_max,
        y_min,
        y_max,
    })
}

fn parse_range(raw: &str) -> Result<(u32, u32)> {
    let parts = raw.split('-').collect::<Vec<_>>();
    if parts.len() != 2 {
        bail!("bad range '{raw}'");
    }
    let min = parts[0].trim().parse::<u32>()?;
    let max = parts[1].trim().parse::<u32>()?;
    if min > max {
        bail!("bad range '{raw}' (min must be <= max)");
    }
    Ok((min, max))
}

fn build_url(template: &str, airport: &str, zoom: u32, x: u32, y: u32, ext: &str) -> String {
    let rel_path = format!("{airport}/{zoom}/{x}_{y}.{ext}");
    template
        .replace("{airport}", airport)
        .replace("{z}", &zoom.to_string())
        .replace("{x}", &x.to_string())
        .replace("{y}", &y.to_string())
        .replace("{ext}", ext)
        .replace("{path}", &rel_path)
}

fn download_job(
    client: &Client,
    job: &DownloadJob,
    retries: u32,
    overwrite_existing: bool,
) -> Result<()> {
    if !overwrite_existing && valid_download(&job.dst, job.min_bytes)? {
        return Ok(());
    }
    if let Some(parent) = job.dst.parent() {
        fs::create_dir_all(parent)?;
    }
    let tmp = job.dst.with_extension(format!(
        "{}.part",
        job.dst
            .extension()
            .and_then(|v| v.to_str())
            .unwrap_or_default()
    ));

    let mut last_error = None;
    for attempt in 1..=retries.max(1) {
        match client.get(&job.url).send() {
            Ok(response) => {
                let response = response.error_for_status()?;
                let bytes = response.bytes()?;
                let mut handle = fs::File::create(&tmp)?;
                handle.write_all(&bytes)?;
                drop(handle);
                fs::rename(&tmp, &job.dst)?;
                if valid_download(&job.dst, job.min_bytes)? {
                    return Ok(());
                }
                let _ = fs::remove_file(&job.dst);
                last_error = Some(anyhow!("too small (<{} bytes)", job.min_bytes));
            }
            Err(err) => {
                last_error = Some(err.into());
            }
        }
        let _ = fs::remove_file(&tmp);
        if attempt < retries {
            std::thread::sleep(Duration::from_millis(1500));
        }
    }
    Err(last_error.unwrap_or_else(|| anyhow!("download failed")))
}

fn valid_download(path: &Path, min_bytes: u64) -> Result<bool> {
    Ok(path.exists() && path.is_file() && path.metadata()?.len() >= min_bytes)
}

fn parse_xy_name(path: &Path) -> Option<(u32, u32)> {
    let stem = path.file_stem()?.to_str()?;
    let parts = stem.split('_').collect::<Vec<_>>();
    if parts.len() != 2 {
        return None;
    }
    let x = parts[0].parse::<u32>().ok()?;
    let y = parts[1].parse::<u32>().ok()?;
    Some((x, y))
}

fn resolve_airport_dir(root: &Path, airport: &str) -> PathBuf {
    let direct = root.join(airport);
    if direct.exists() {
        return direct;
    }
    if airport == "KCRQ" {
        let alias = root.join("KRCQ");
        if alias.exists() {
            return alias;
        }
    }
    direct
}

fn collect_airport_dirs(root: &Path) -> Result<Vec<PathBuf>> {
    let mut out = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if let Some(name) = path.file_name().and_then(|v| v.to_str()) {
                if name.starts_with('K') && name.len() == 4 {
                    out.push(path);
                }
            }
        }
    }
    out.sort();
    Ok(out)
}

fn best_source_tile(
    preferred_airport_dir: &Path,
    z: u32,
    x: u32,
    y: u32,
    airport_dirs: &[PathBuf],
) -> Option<PathBuf> {
    let rel = PathBuf::from(z.to_string()).join(format!("{x}_{y}.png"));
    let mut candidates = Vec::new();

    let primary = preferred_airport_dir.join(&rel);
    if valid_png(&primary) {
        candidates.push(primary);
    }

    for airport_dir in airport_dirs {
        if airport_dir == preferred_airport_dir {
            continue;
        }
        let candidate = airport_dir.join(&rel);
        if valid_png(&candidate) {
            candidates.push(candidate);
        }
    }

    candidates
        .into_iter()
        .max_by_key(|path| path.metadata().map(|m| m.len()).unwrap_or_default())
}

fn valid_png(path: &Path) -> bool {
    path.exists() && path.metadata().map(|m| m.len()).unwrap_or(0) > 100
}

fn parse_target_metadata(name: &str) -> Result<TargetMeta> {
    let (prefix, suffix) = name
        .split_once("_fromZ")
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?;
    let out_z = prefix
        .strip_prefix('Z')
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?
        .parse::<u32>()?;
    let (base_part, remainder) = suffix
        .split_once("_r")
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?;
    let base_z = base_part.parse::<u32>()?;
    let (_row_part, remainder) = remainder
        .split_once("_c")
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?;
    let (_col_part, remainder) = remainder
        .split_once("_baseX")
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?;
    let (bx_part, by_part) = remainder
        .split_once("_baseY")
        .ok_or_else(|| anyhow!("bad target filename pattern: {name}"))?;
    let by_part = by_part.strip_suffix(".tif").unwrap_or(by_part);
    Ok(TargetMeta {
        out_z,
        base_z,
        bx: bx_part.parse::<u32>()?,
        by: by_part.parse::<u32>()?,
    })
}

fn quality_score_gdal(tif_path: &Path) -> Result<f64> {
    let gdalinfo = resolve_gdal("gdalinfo.exe");
    let output = Command::new(&gdalinfo)
        .arg("-json")
        .arg("-stats")
        .arg(to_win_path(tif_path))
        .output()
        .with_context(|| format!("failed to run {}", gdalinfo))?;
    if !output.status.success() {
        return Ok(-1.0);
    }
    let payload: Value = serde_json::from_slice(&output.stdout)?;
    let Some(bands) = payload.get("bands").and_then(|v| v.as_array()) else {
        return Ok(-1.0);
    };
    if bands.is_empty() {
        return Ok(-1.0);
    }

    let mut score = 0.0f64;
    for band in bands.iter().take(3) {
        let md = band
            .get("metadata")
            .and_then(|v| v.get(""))
            .and_then(|v| v.as_object());
        let min = md
            .and_then(|m| m.get("STATISTICS_MINIMUM"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let max = md
            .and_then(|m| m.get("STATISTICS_MAXIMUM"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let mean = md
            .and_then(|m| m.get("STATISTICS_MEAN"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        let stddev = md
            .and_then(|m| m.get("STATISTICS_STDDEV"))
            .and_then(|v| v.as_str())
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        score += (stddev * 4.0) + ((max - min) * 0.2) + (mean * 0.05);
    }
    score += (tif_path.metadata()?.len() as f64 / 1_000_000.0).min(200.0);
    Ok(score)
}

fn build_from_zoom(
    airport_dir: &Path,
    airport_dirs: &[PathBuf],
    tif_path: &Path,
    meta: TargetMeta,
    src_z: u32,
) -> Result<bool> {
    let (minx, miny, maxx, maxy, factor) = cone_bounds(meta.out_z, meta.base_z, meta.bx, meta.by);
    let (x_min, x_max, y_min, y_max) = source_tile_range(minx, miny, maxx, maxy, src_z);
    let src_dir = airport_dir.join(src_z.to_string());
    if !src_dir.exists() {
        return Ok(false);
    }

    let mut source_tiles = Vec::new();
    for y in y_min..=y_max {
        for x in x_min..=x_max {
            let Some(tile) = best_source_tile(airport_dir, src_z, x, y, airport_dirs) else {
                return Ok(false);
            };
            source_tiles.push(tile);
        }
    }

    let gdalbuildvrt = resolve_gdal("gdalbuildvrt.exe");
    let gdal_translate = resolve_gdal("gdal_translate.exe");
    let out_px = factor * 256;
    let out_vrt = tif_path.with_extension("vrt");

    let mut list_file = NamedTempFile::new()?;
    for tile in &source_tiles {
        writeln!(list_file, "{}", to_win_path(tile))?;
    }
    list_file.flush()?;

    let vrt_status = Command::new(&gdalbuildvrt)
        .arg("-q")
        .arg("-input_file_list")
        .arg(to_win_path(list_file.path()))
        .arg(to_win_path(&out_vrt))
        .status()?;
    if !vrt_status.success() {
        return Ok(false);
    }

    let translate_status = Command::new(&gdal_translate)
        .arg("-q")
        .arg("-of")
        .arg("GTiff")
        .arg("-a_srs")
        .arg("EPSG:3857")
        .arg("-co")
        .arg("TILED=YES")
        .arg("-co")
        .arg("COMPRESS=DEFLATE")
        .arg("-co")
        .arg("PREDICTOR=2")
        .arg("-co")
        .arg("ZLEVEL=6")
        .arg("-co")
        .arg("BIGTIFF=IF_SAFER")
        .arg("-projwin")
        .arg(format!("{minx:.15}"))
        .arg(format!("{maxy:.15}"))
        .arg(format!("{maxx:.15}"))
        .arg(format!("{miny:.15}"))
        .arg("-outsize")
        .arg(out_px.to_string())
        .arg(out_px.to_string())
        .arg(to_win_path(&out_vrt))
        .arg(to_win_path(tif_path))
        .status()?;
    if !translate_status.success() {
        let _ = fs::remove_file(tif_path);
        return Ok(false);
    }

    Ok(true)
}

fn cone_bounds(out_z: u32, base_z: u32, bx: u32, by: u32) -> (f64, f64, f64, f64, u32) {
    let factor = 1u32 << (out_z - base_z);
    let n = 1u32 << out_z;
    let tile_extent = WEBM_WORLD / n as f64;
    let x0 = bx * factor;
    let y0 = by * factor;
    let minx = -WEBM_HALF + x0 as f64 * tile_extent;
    let maxx = minx + factor as f64 * tile_extent;
    let maxy = WEBM_HALF - y0 as f64 * tile_extent;
    let miny = maxy - factor as f64 * tile_extent;
    (minx, miny, maxx, maxy, factor)
}

fn source_tile_range(
    minx: f64,
    miny: f64,
    maxx: f64,
    maxy: f64,
    src_z: u32,
) -> (u32, u32, u32, u32) {
    let n = 1u32 << src_z;
    let tile_extent = WEBM_WORLD / n as f64;
    let mut x_min = ((minx + WEBM_HALF) / tile_extent).floor() as i64;
    let mut x_max = ((maxx + WEBM_HALF) / tile_extent).ceil() as i64 - 1;
    let mut y_min = ((WEBM_HALF - maxy) / tile_extent).floor() as i64;
    let mut y_max = ((WEBM_HALF - miny) / tile_extent).ceil() as i64 - 1;
    let max_index = n as i64 - 1;
    x_min = x_min.clamp(0, max_index);
    x_max = x_max.clamp(0, max_index);
    y_min = y_min.clamp(0, max_index);
    y_max = y_max.clamp(0, max_index);
    (x_min as u32, x_max as u32, y_min as u32, y_max as u32)
}

fn resolve_gdal(name: &str) -> String {
    for base in ["/mnt/c/OSGeo4W/bin", r"C:\OSGeo4W\bin"] {
        let candidate = Path::new(base).join(name);
        if candidate.exists() {
            return candidate.display().to_string();
        }
    }
    name.to_string()
}

fn to_win_path(path: &Path) -> String {
    let raw = path.display().to_string();
    if raw.starts_with('/') {
        if let Ok(output) = Command::new("wslpath").arg("-w").arg(&raw).output() {
            if output.status.success() {
                if let Ok(text) = String::from_utf8(output.stdout) {
                    return text.trim().to_string();
                }
            }
        }
    }
    raw
}
