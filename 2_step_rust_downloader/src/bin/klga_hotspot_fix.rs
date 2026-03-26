use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use clap::Parser;
use rayon::prelude::*;
use reqwest::blocking::Client;
use step_rust_downloader::write_png_file;

#[derive(Parser, Debug)]
#[command(name = "klga_hotspot_fix")]
#[command(about = "Rust replacement for the legacy KLGA gray hotspot download fixer")]
struct Args {
    #[arg(long)]
    url_template: String,

    #[arg(long, default_value = "C:/Users/ravery/Downloads/AIRPORT_TILES")]
    root: PathBuf,

    #[arg(long, default_value_t = 24)]
    workers: usize,

    #[arg(long, default_value_t = 4)]
    retries: usize,

    #[arg(long, default_value_t = 30)]
    timeout: u64,

    #[arg(long)]
    include_pgw: bool,

    #[arg(long)]
    dry_run: bool,
}

#[derive(Clone)]
struct DownloadJob {
    airport: &'static str,
    z: u32,
    x: u32,
    y: u32,
    ext: &'static str,
    output: PathBuf,
    min_bytes: u64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let client = Client::builder()
        .timeout(Duration::from_secs(args.timeout))
        .build()?;

    let jobs = build_jobs(&args.root, args.include_pgw);
    println!("Planned files: {}", jobs.len());

    if args.dry_run {
        for job in jobs.iter().take(30) {
            println!(
                "{} -> {}",
                build_url(&args.url_template, job),
                job.output.display()
            );
        }
        if jobs.len() > 30 {
            println!("... and {} more", jobs.len() - 30);
        }
        return Ok(());
    }

    rayon::ThreadPoolBuilder::new()
        .num_threads(args.workers)
        .build_global()
        .ok();

    let ok = AtomicUsize::new(0);
    let fail = AtomicUsize::new(0);
    let processed = AtomicUsize::new(0);
    let failures = jobs
        .par_iter()
        .filter_map(|job| {
            let result = download_job(&client, &args.url_template, job, args.retries);
            let done = processed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == jobs.len() || done % 250 == 0 {
                println!(
                    "Progress {done}/{} ok={} fail={}",
                    jobs.len(),
                    ok.load(Ordering::Relaxed),
                    fail.load(Ordering::Relaxed)
                );
            }

            match result {
                Ok(()) => {
                    ok.fetch_add(1, Ordering::Relaxed);
                    None
                }
                Err(error) => {
                    fail.fetch_add(1, Ordering::Relaxed);
                    Some(error)
                }
            }
        })
        .collect::<Vec<_>>();

    if !failures.is_empty() {
        let report = args
            .root
            .join("CODE")
            .join("reports")
            .join("klga_hotspot_download_failures.txt");
        if let Some(parent) = report.parent() {
            fs::create_dir_all(parent)?;
        }
        let report_body = failures
            .iter()
            .map(|error| error.to_string())
            .collect::<Vec<_>>()
            .join("\n");
        fs::write(&report, report_body)?;
        anyhow::bail!("Failures written to {}", report.display());
    }

    println!("Download replace complete.");
    Ok(())
}

fn build_jobs(root: &Path, include_pgw: bool) -> Vec<DownloadJob> {
    let mut jobs = Vec::new();
    add_range(
        &mut jobs,
        "KLGA",
        14,
        4832,
        4863,
        6176,
        6207,
        root,
        include_pgw,
    );
    add_range(
        &mut jobs,
        "KLGA",
        15,
        9664,
        9727,
        12352,
        12415,
        root,
        include_pgw,
    );
    jobs
}

fn add_range(
    jobs: &mut Vec<DownloadJob>,
    airport: &'static str,
    z: u32,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
    root: &Path,
    include_pgw: bool,
) {
    for x in x_min..=x_max {
        for y in y_min..=y_max {
            jobs.push(DownloadJob {
                airport,
                z,
                x,
                y,
                ext: "png",
                output: root
                    .join(airport)
                    .join(z.to_string())
                    .join(format!("{x}_{y}.png")),
                min_bytes: 100,
            });
            if include_pgw {
                jobs.push(DownloadJob {
                    airport,
                    z,
                    x,
                    y,
                    ext: "pgw",
                    output: root
                        .join(airport)
                        .join(z.to_string())
                        .join(format!("{x}_{y}.pgw")),
                    min_bytes: 20,
                });
            }
        }
    }
}

fn build_url(template: &str, job: &DownloadJob) -> String {
    template
        .replace("{airport}", job.airport)
        .replace("{z}", &job.z.to_string())
        .replace("{x}", &job.x.to_string())
        .replace("{y}", &job.y.to_string())
        .replace("{ext}", job.ext)
        .replace(
            "{path}",
            &format!("{}/{}/{}_{}.{}", job.airport, job.z, job.x, job.y, job.ext),
        )
}

fn download_job(client: &Client, template: &str, job: &DownloadJob, retries: usize) -> Result<()> {
    if let Some(parent) = job.output.parent() {
        fs::create_dir_all(parent)?;
    }

    let url = build_url(template, job);
    let tmp = job.output.with_extension(format!("{}.part", job.ext));
    let mut last_error = None;

    for attempt in 0..retries.max(1) {
        match client
            .get(&url)
            .header("User-Agent", "klga-hotspot-fix-rust/1.0")
            .send()
        {
            Ok(response) => {
                let response = response.error_for_status().context("bad HTTP status")?;
                let bytes = response.bytes().context("reading response body")?;
                if job.ext == "png" {
                    write_png_file(&tmp, &bytes)?;
                } else {
                    fs::write(&tmp, &bytes)?;
                }
                if bytes.len() as u64 >= job.min_bytes {
                    fs::rename(&tmp, &job.output)?;
                    return Ok(());
                }
                let _ = fs::remove_file(&tmp);
                last_error = Some(anyhow::anyhow!("too small: {}", job.output.display()));
            }
            Err(error) => {
                let _ = fs::remove_file(&tmp);
                last_error = Some(error.into());
            }
        }

        if attempt + 1 < retries {
            thread::sleep(Duration::from_millis(1200));
        }
    }

    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("download failed: {url}")))
}
