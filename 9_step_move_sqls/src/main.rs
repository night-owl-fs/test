use std::fs;
use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;
use pipeline_core::{ProgressReporter, StepTimer};
use walkdir::WalkDir;

use move_sqls_rust::{
    clear_destination_config, default_destination_config_path, is_sql_artifact,
    load_destination_config, resolve_output_dir, unique_target_path, MoveRecord,
};

#[derive(Parser, Debug)]
#[command(name = "move_sqls_rust")]
#[command(about = "Move/copy final SQLite/GeoPackage artifacts into organized output folder")]
struct Args {
    #[arg(long)]
    input_dir: PathBuf,

    #[arg(long)]
    output_dir: Option<PathBuf>,

    #[arg(long)]
    set_default_destination: Option<PathBuf>,

    #[arg(long)]
    clear_default_destination: bool,

    #[arg(long)]
    show_default_destination: bool,

    #[arg(long)]
    config: Option<PathBuf>,

    #[arg(long)]
    prefix: Option<String>,

    #[arg(long)]
    copy: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    report: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config_path = args
        .config
        .clone()
        .unwrap_or_else(default_destination_config_path);
    if args.clear_default_destination {
        clear_destination_config(&config_path)?;
        println!(
            "Cleared default destination config at {}",
            config_path.display()
        );
    }
    if args.show_default_destination {
        match load_destination_config(&config_path)? {
            Some(config) => println!("default_destination={}", config.default_destination),
            None => println!("default_destination=(not set)"),
        }
    }

    let output_dir = resolve_output_dir(
        &args.input_dir,
        args.output_dir.as_deref(),
        args.set_default_destination.as_deref(),
        &config_path,
    )?;
    let timer = StepTimer::new(9, "9_step_move_sqls", output_dir.clone());
    fs::create_dir_all(&output_dir)?;

    let sources = WalkDir::new(&args.input_dir)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(|entry| entry.into_path())
        .filter(|path| is_sql_artifact(path))
        .collect::<Vec<_>>();
    let total_sources = sources.len().max(1);
    let progress = ProgressReporter::new(9, "9_step_move_sqls", total_sources);
    progress.start(Some("Moving SQL artifacts".to_string()));

    let mut records = Vec::new();
    for (index, source) in sources.into_iter().enumerate() {
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
        let target = unique_target_path(&output_dir, &file_name);
        let action = if args.copy { "copy" } else { "move" };

        if args.dry_run {
            println!(
                "DRY-RUN {action}: {} -> {}",
                source.display(),
                target.display()
            );
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
        progress.update(
            index + 1,
            Some(total_sources),
            0,
            Some(format!("Handled {}", file_name)),
        );
    }

    let report_path = args
        .report
        .unwrap_or_else(|| output_dir.join("move_sqls_report.json"));
    fs::write(&report_path, serde_json::to_string_pretty(&records)?)?;

    println!("Move SQL stage complete");
    println!("destination={}", output_dir.display());
    println!("config={}", config_path.display());
    println!("artifacts_processed={}", records.len());
    println!("report={}", report_path.display());
    progress.finish(
        records.len(),
        Some(total_sources),
        0,
        Some("Move SQL stage complete".to_string()),
    );
    let _ = timer.finish(
        Some(total_sources),
        Some(records.len()),
        Some(0),
        format!("Moved/copied SQL artifacts into {}", output_dir.display()),
    )?;
    Ok(())
}
