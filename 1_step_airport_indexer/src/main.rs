use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use pipeline_core::{build_airport_cone_to_heaven_report, render_cone_spec_text};

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
    let db_path = args.db.canonicalize().unwrap_or(args.db.clone());
    let report = build_airport_cone_to_heaven_report(&db_path, &args.icao)?;

    if !report.airports_missing.is_empty() {
        eprintln!("[WARN] Missing ICAOs: {:?}", report.airports_missing);
    }

    if let Some(specs_dir) = &args.out_specs_dir {
        fs::create_dir_all(specs_dir)?;
        for airport in &report.airports {
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

    Ok(())
}
