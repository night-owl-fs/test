use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;
use walkdir::WalkDir;

use move_sqls_rust::{is_sql_artifact, unique_target_path};

#[derive(Parser, Debug)]
#[command(name = "move_sqls_rust")]
#[command(about = "Move/copy final SQLite/GeoPackage artifacts into organized output folder")]
struct Args {
    #[arg(long)]
    input_dir: PathBuf,

    #[arg(long)]
    output_dir: PathBuf,

    #[arg(long)]
    prefix: Option<String>,

    #[arg(long)]
    copy: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    report: Option<PathBuf>,
}

#[derive(Debug, Serialize)]
struct MoveRecord {
    source: String,
    target: String,
    action: String,
}

fn main() -> Result<()> {
    let args = Args::parse();
    fs::create_dir_all(&args.output_dir)?;

    let mut records = Vec::new();
    for entry in WalkDir::new(&args.input_dir).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let source = entry.into_path();
        if !is_sql_artifact(&source) {
            continue;
        }

        let file_name = source
            .file_name()
            .and_then(|x| x.to_str())
            .unwrap_or("artifact.sqlite")
            .to_string();
        let file_name = if let Some(prefix) = &args.prefix {
            format!("{}{}", prefix, file_name)
        } else {
            file_name
        };
        let target = unique_target_path(&args.output_dir, &file_name);
        let action = if args.copy { "copy" } else { "move" };

        if args.dry_run {
            println!("DRY-RUN {action}: {} -> {}", source.display(), target.display());
        } else if args.copy {
            fs::copy(&source, &target)?;
        } else {
            fs::rename(&source, &target)?;
        }

        records.push(MoveRecord {
            source: source.display().to_string(),
            target: target.display().to_string(),
            action: action.to_string(),
        });
    }

    let report_path = args
        .report
        .unwrap_or_else(|| args.output_dir.join("move_sqls_report.json"));
    fs::write(&report_path, serde_json::to_string_pretty(&records)?)?;

    println!("Move SQL stage complete");
    println!("artifacts_processed={}", records.len());
    println!("report={}", report_path.display());
    Ok(())
}
