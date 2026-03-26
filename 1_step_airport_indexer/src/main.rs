use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use pipeline_core::{
    build_airport_cone_to_heaven_report, render_cone_spec_text, ProgressReporter, StepTimer,
};

#[derive(Parser, Debug)]
#[command(name = "step_airport_indexer")]
#[command(about = "Build cone jobs from the airport SQLite index", long_about = None)]
struct Args {
    #[arg(long)]
    db: PathBuf,

    #[arg(long, num_args = 1..)]
    icao: Vec<String>,

    #[arg(long)]
    out_spec: Option<PathBuf>,

    #[arg(long)]
    out_specs_dir: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let timing_root = args
        .out_spec
        .as_ref()
        .and_then(|path| path.parent().map(PathBuf::from))
        .or_else(|| args.out_specs_dir.clone())
        .unwrap_or_else(|| PathBuf::from("."));
    let timer = StepTimer::new(1, "1_step_airport_indexer", timing_root);
    let db_path = args.db.canonicalize().unwrap_or(args.db.clone());
    let report = build_airport_cone_to_heaven_report(&db_path, &args.icao)?;
    let total_airports = report.airports.len().max(1);
    let progress = ProgressReporter::new(1, "1_step_airport_indexer", total_airports);
    progress.start(Some("Generating cone jobs".to_string()));

    if !report.airports_missing.is_empty() {
        eprintln!("[WARN] Missing ICAOs: {:?}", report.airports_missing);
    }

    if let Some(specs_dir) = &args.out_specs_dir {
        fs::create_dir_all(specs_dir)?;
        for (index, airport) in report.airports.iter().enumerate() {
            let airport_jobs = report
                .jobs
                .iter()
                .filter(|j| j.icao == airport.icao)
                .cloned()
                .collect::<Vec<_>>();
            let text = render_cone_spec_text(airport, &airport_jobs);
            let path = specs_dir.join(format!("{}_spec.txt", airport.icao));
            fs::write(&path, text)?;
            println!("Wrote text cone spec: {}", path.display());
            progress.update(
                index + 1,
                Some(total_airports),
                report.airports_missing.len(),
                Some(format!("Wrote {}", airport.icao)),
            );
        }
    }

    let out_json = serde_json::to_string_pretty(&report.jobs)?;
    if let Some(path) = args.out_spec {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&path, out_json)?;
        println!(
            "Wrote {} cone jobs for {} airports to {}",
            report.jobs.len(),
            report.airports_found.len(),
            path.display()
        );
    } else {
        println!("{out_json}");
    }

    progress.finish(
        report.airports.len(),
        Some(total_airports),
        report.airports_missing.len(),
        Some(format!("Generated {} cone jobs", report.jobs.len())),
    );
    let _ = timer.finish(
        Some(report.airports_requested.len()),
        Some(report.jobs.len()),
        Some(report.airports_missing.len()),
        format!(
            "Generated cone jobs for {} airports (missing={})",
            report.airports_found.len(),
            report.airports_missing.len()
        ),
    )?;

    Ok(())
}
