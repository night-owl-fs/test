use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use clap::Parser;
use pipeline_core::{ProgressReporter, StepTimer};
use rayon::prelude::*;
use walkdir::WalkDir;

use pgw_sidecar_maker::{is_supported_tile, parse_xy_and_zoom_from_path, worldfile_text};

#[derive(Parser, Debug)]
#[command(name = "pgw_sidecar_maker")]
#[command(about = "Generate PGW sidecars for XYZ tile files")]
struct Args {
    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    zoom: Option<u8>,

    #[arg(long, default_value_t = true)]
    recursive: bool,

    #[arg(long, default_value_t = false)]
    write_image_ext_pgw: bool,

    #[arg(long)]
    overwrite: bool,

    #[arg(long)]
    workers: Option<usize>,
}

fn output_paths(tile_path: &Path, write_image_ext_pgw: bool) -> Vec<PathBuf> {
    let mut outs = vec![tile_path.with_extension("pgw")];
    if write_image_ext_pgw {
        outs.push(PathBuf::from(format!("{}.pgw", tile_path.display())));
    }
    outs
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timer = StepTimer::new(4, "4_step_pgw_rust", args.input.clone());
    if let Some(workers) = args.workers {
        rayon::ThreadPoolBuilder::new()
            .num_threads(workers)
            .build_global()
            .expect("failed to configure rayon");
    }

    let files = if args.recursive {
        WalkDir::new(&args.input)
            .follow_links(true)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|e| e.file_type().is_file() && is_supported_tile(e.path()))
            .map(|e| e.into_path())
            .collect::<Vec<_>>()
    } else {
        fs::read_dir(&args.input)?
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.is_file() && is_supported_tile(p))
            .collect::<Vec<_>>()
    };

    let total = files.len();
    let progress = ProgressReporter::new(4, "4_step_pgw_rust", total.max(1));
    progress.start(Some("Generating PGW sidecars".to_string()));
    let written = AtomicUsize::new(0);
    let skipped_existing = AtomicUsize::new(0);
    let skipped_bad_name = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);
    let processed = AtomicUsize::new(0);

    files.par_iter().for_each(|tile_path| {
        let Some((zoom, x, y)) = parse_xy_and_zoom_from_path(tile_path, args.zoom) else {
            skipped_bad_name.fetch_add(1, Ordering::Relaxed);
            let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == total || done % 250 == 0 {
                progress.update(
                    done,
                    Some(total.max(1)),
                    errors.load(Ordering::Relaxed),
                    Some("Generating PGW sidecars".to_string()),
                );
            }
            return;
        };
        let pgw_text = worldfile_text(zoom, x, y);
        for out in output_paths(tile_path, args.write_image_ext_pgw) {
            if out.exists() && !args.overwrite {
                skipped_existing.fetch_add(1, Ordering::Relaxed);
                continue;
            }
            if let Some(parent) = out.parent() {
                if let Err(err) = fs::create_dir_all(parent) {
                    eprintln!("failed to create {}: {}", parent.display(), err);
                    errors.fetch_add(1, Ordering::Relaxed);
                    continue;
                }
            }
            match fs::write(&out, pgw_text.as_bytes()) {
                Ok(_) => {
                    written.fetch_add(1, Ordering::Relaxed);
                }
                Err(err) => {
                    eprintln!("failed to write {}: {}", out.display(), err);
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
        if done == total || done % 250 == 0 {
            progress.update(
                done,
                Some(total.max(1)),
                errors.load(Ordering::Relaxed),
                Some("Generating PGW sidecars".to_string()),
            );
        }
    });

    println!("PGW stage complete");
    println!("input={}", args.input.display());
    println!("tiles_seen={total}");
    println!("written={}", written.load(Ordering::Relaxed));
    println!(
        "skipped_existing={}",
        skipped_existing.load(Ordering::Relaxed)
    );
    println!(
        "skipped_bad_name={}",
        skipped_bad_name.load(Ordering::Relaxed)
    );
    println!("errors={}", errors.load(Ordering::Relaxed));
    progress.finish(
        total,
        Some(total.max(1)),
        errors.load(Ordering::Relaxed),
        Some("PGW stage complete".to_string()),
    );
    let _ = timer.finish(
        Some(total),
        Some(written.load(Ordering::Relaxed)),
        Some(errors.load(Ordering::Relaxed)),
        format!(
            "Generated {} PGW files (skipped_existing={} skipped_bad_name={})",
            written.load(Ordering::Relaxed),
            skipped_existing.load(Ordering::Relaxed),
            skipped_bad_name.load(Ordering::Relaxed)
        ),
    )?;

    Ok(())
}
