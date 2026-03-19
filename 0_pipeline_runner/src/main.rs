use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command as ProcessCommand;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Parser, Subcommand};
use pipeline_core::{
    build_airport_index_report, build_download_manifest, default_cone_profiles, ConeJob,
    DownloadManifest,
};
use pipeline_runner::{PipelinePlan, PipelineStepPlan};
use walkdir::WalkDir;

#[derive(Parser, Debug)]
#[command(name = "pipeline_runner")]
#[command(about = "Orchestrate pipeline prep outputs for Rust + Electron integration")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand, Debug)]
enum Command {
    Prepare(PrepareArgs),
    Build(PrepareArgs),
}

#[derive(Parser, Debug)]
struct PrepareArgs {
    #[arg(long)]
    db: PathBuf,

    #[arg(long, num_args = 1..)]
    icao: Vec<String>,

    #[arg(long, default_value = "crates/0_pipeline_runner/generated")]
    out_dir: PathBuf,

    #[arg(long, default_value_t = 1)]
    three_by_three_radius: i32,

    #[arg(long, default_value_t = 2)]
    five_by_five_radius: i32,

    #[arg(
        long,
        default_value = "https://tiles.example.invalid/{z}/{x}/{y}.png"
    )]
    url_template: String,

    #[arg(long)]
    zooms: Option<String>,

    #[arg(long)]
    workers: Option<usize>,

    #[arg(long)]
    resume: bool,

    #[arg(long)]
    dry_run: bool,

    #[arg(long)]
    distributed: bool,

    #[arg(long)]
    nodes: Vec<String>,

    #[arg(long)]
    print_plan: bool,

    #[arg(long)]
    allow_rerun: bool,

    #[arg(long)]
    force_unlock: bool,
}

#[derive(Debug, Clone)]
struct PipelinePaths {
    out_dir: PathBuf,
    state_dir: PathBuf,
    step_state_dir: PathBuf,
    pipeline_lock_file: PathBuf,
    state_csv: PathBuf,
    context_txt: PathBuf,
    jobs_file: PathBuf,
    manifest_file: PathBuf,
    plan_file: PathBuf,
    raw_tiles_dir: PathBuf,
    brcj_dir: PathBuf,
    geotiff_dir: PathBuf,
    imagery_dir: PathBuf,
    imagery_db: PathBuf,
    imagery_trim_db: PathBuf,
    imagery_merge_db: PathBuf,
    final_dir: PathBuf,
    generative_plan: PathBuf,
}

#[derive(Debug)]
struct PreparedRun {
    plan: PipelinePlan,
    paths: PipelinePaths,
    expected_job_count: usize,
    expected_tile_count: usize,
}

#[derive(Debug, Default, Clone)]
struct StepMetrics {
    input_count: Option<usize>,
    output_count: Option<usize>,
    note: String,
}

struct RunLock {
    path: PathBuf,
}

impl Drop for RunLock {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn parse_zoom_list(raw: &str) -> Result<Vec<u32>> {
    let mut zooms = Vec::new();
    for part in raw.split(',').map(str::trim).filter(|x| !x.is_empty()) {
        if let Some((start, end)) = part.split_once('-') {
            let start = start
                .trim()
                .parse::<u32>()
                .with_context(|| format!("Invalid zoom start: {start}"))?;
            let end = end
                .trim()
                .parse::<u32>()
                .with_context(|| format!("Invalid zoom end: {end}"))?;
            if start > end {
                bail!("Invalid zoom range {part}: start > end");
            }
            zooms.extend(start..=end);
        } else {
            zooms.push(
                part.parse::<u32>()
                    .with_context(|| format!("Invalid zoom value: {part}"))?,
            );
        }
    }
    zooms.sort_unstable();
    zooms.dedup();
    Ok(zooms)
}

fn derive_paths(out_dir: &Path) -> PipelinePaths {
    let state_dir = out_dir.join("state");
    let step_state_dir = state_dir.join("steps");
    let imagery_dir = out_dir.join("imagery");
    let final_dir = out_dir.join("final");

    PipelinePaths {
        out_dir: out_dir.to_path_buf(),
        state_dir: state_dir.clone(),
        step_state_dir,
        pipeline_lock_file: state_dir.join("pipeline.lock"),
        state_csv: state_dir.join("pipeline_state.csv"),
        context_txt: state_dir.join("pipeline_context.txt"),
        jobs_file: out_dir.join("airport_cones.json"),
        manifest_file: out_dir.join("download_manifest.json"),
        plan_file: out_dir.join("pipeline_plan.json"),
        raw_tiles_dir: out_dir.join("tiles_raw"),
        brcj_dir: out_dir.join("tiles_brcj"),
        geotiff_dir: out_dir.join("geotiff"),
        imagery_db: imagery_dir.join("imagery.gpkg"),
        imagery_trim_db: imagery_dir.join("imagery_trim.gpkg"),
        imagery_merge_db: imagery_dir.join("imagery_merged.gpkg"),
        imagery_dir,
        final_dir: final_dir.clone(),
        generative_plan: out_dir.join("generative").join("repair_plan.json"),
    }
}

fn build_step_plan(paths: &PipelinePaths, args: &PrepareArgs) -> Result<PipelinePlan> {
    let airports = args
        .icao
        .iter()
        .map(|x| x.trim().to_uppercase())
        .collect::<Vec<_>>();
    let mut step1_cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "-p".to_string(),
        "step_airport_indexer".to_string(),
        "--bin".to_string(),
        "step_airport_indexer".to_string(),
        "--".to_string(),
        "--db".to_string(),
        args.db.display().to_string(),
        "--icao".to_string(),
    ];
    step1_cmd.extend(airports.clone());
    step1_cmd.extend([
        "--out-spec".to_string(),
        paths.jobs_file.display().to_string(),
        "--out-specs-dir".to_string(),
        args.out_dir.join("cone_specs").display().to_string(),
    ]);

    let worker_count = args.workers.unwrap_or(8).to_string();
    let mut step2_cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "-p".to_string(),
        "step_rust_downloader".to_string(),
        "--bin".to_string(),
        "step_rust_downloader".to_string(),
        "--".to_string(),
        "--spec".to_string(),
        paths.jobs_file.display().to_string(),
        "--out-manifest".to_string(),
        paths.manifest_file.display().to_string(),
        "--url-template".to_string(),
        args.url_template.clone(),
        "--download-root".to_string(),
        paths.raw_tiles_dir.display().to_string(),
        "--workers".to_string(),
        worker_count.clone(),
    ];
    if args.resume {
        step2_cmd.push("--resume".to_string());
    }
    if args.dry_run {
        step2_cmd.push("--dry-run".to_string());
    }

    let mut step5_cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "-p".to_string(),
        "cone_to_heaven_rust".to_string(),
        "--".to_string(),
        "--jobs".to_string(),
        paths.jobs_file.display().to_string(),
        "--tiles-root".to_string(),
        paths.brcj_dir.display().to_string(),
        "--out-dir".to_string(),
        paths.geotiff_dir.display().to_string(),
        "--strict".to_string(),
    ];
    if args.dry_run {
        step5_cmd.push("--dry-run".to_string());
    }

    let mut step6_cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "-p".to_string(),
        "rust_imagery_tiler".to_string(),
        "--".to_string(),
        "--input-dir".to_string(),
        paths.geotiff_dir.display().to_string(),
        "--output-db".to_string(),
        paths.imagery_db.display().to_string(),
        "--print-command-file".to_string(),
        paths
            .imagery_dir
            .join("imagery_tiler_command.txt")
            .display()
            .to_string(),
    ];
    if args.dry_run {
        step6_cmd.push("--dry-run".to_string());
    }

    let mut step10_cmd = vec![
        "cargo".to_string(),
        "run".to_string(),
        "-p".to_string(),
        "generative_fix_rust".to_string(),
        "--".to_string(),
        "--tiles-root".to_string(),
        paths.brcj_dir.display().to_string(),
        "--out-plan".to_string(),
        paths.generative_plan.display().to_string(),
    ];
    if let Some(raw_zooms) = &args.zooms {
        for zoom in parse_zoom_list(raw_zooms)? {
            step10_cmd.push("--zoom".to_string());
            step10_cmd.push(zoom.to_string());
        }
    }

    let steps = vec![
        PipelineStepPlan {
            id: "1_step_airport_indexer".to_string(),
            status: "wired".to_string(),
            description: "Generate cone jobs from the airport database".to_string(),
            command: step1_cmd,
        },
        PipelineStepPlan {
            id: "2_step_rust_downloader".to_string(),
            status: "wired".to_string(),
            description: "Expand jobs into per-tile download manifest".to_string(),
            command: step2_cmd,
        },
        PipelineStepPlan {
            id: "3_step_rust_brcj".to_string(),
            status: "wired".to_string(),
            description: "BRCJ image enhancement stage".to_string(),
            command: {
                let mut cmd = vec![
                    "cargo".to_string(),
                    "run".to_string(),
                    "-p".to_string(),
                    "brcj_rust".to_string(),
                    "--bin".to_string(),
                    "brcj_rust".to_string(),
                    "--".to_string(),
                    "--input".to_string(),
                    paths.raw_tiles_dir.display().to_string(),
                    "--output".to_string(),
                    paths.brcj_dir.display().to_string(),
                    "--zoom".to_string(),
                    "18".to_string(),
                    "--workers".to_string(),
                    worker_count.clone(),
                ];
                if args.allow_rerun {
                    cmd.push("--overwrite".to_string());
                }
                cmd
            },
        },
        PipelineStepPlan {
            id: "4_step_pgw_rust".to_string(),
            status: "wired".to_string(),
            description: "Generate PGW sidecars for georeferencing".to_string(),
            command: {
                let mut cmd = vec![
                    "cargo".to_string(),
                    "run".to_string(),
                    "-p".to_string(),
                    "pgw_sidecar_maker".to_string(),
                    "--".to_string(),
                    "--input".to_string(),
                    paths.brcj_dir.display().to_string(),
                    "--recursive".to_string(),
                ];
                if args.allow_rerun {
                    cmd.push("--overwrite".to_string());
                }
                cmd
            },
        },
        PipelineStepPlan {
            id: "5_step_geotiff_to_heaven".to_string(),
            status: "wired".to_string(),
            description: "Build GeoTIFF mosaics from job specs".to_string(),
            command: step5_cmd,
        },
        PipelineStepPlan {
            id: "6_step_rust_imagery_tiler".to_string(),
            status: "wired".to_string(),
            description: "Call Cesium imagery-tiler for GeoTIFF inputs".to_string(),
            command: step6_cmd,
        },
        PipelineStepPlan {
            id: "7_step_rust_adios_trim".to_string(),
            status: "wired".to_string(),
            description: "Re-encode tile blobs for optimized database size".to_string(),
            command: vec![
                "cargo".to_string(),
                "run".to_string(),
                "-p".to_string(),
                "rust_trim_job".to_string(),
                "--".to_string(),
                "--input".to_string(),
                paths.imagery_db.display().to_string(),
                "--output".to_string(),
                paths.imagery_trim_db.display().to_string(),
                "--quality".to_string(),
                "90".to_string(),
                "--force".to_string(),
            ],
        },
        PipelineStepPlan {
            id: "8_step_merge_tiles".to_string(),
            status: "wired".to_string(),
            description: "Merge optimized tile databases".to_string(),
            command: vec![
                "cargo".to_string(),
                "run".to_string(),
                "-p".to_string(),
                "merge_tiles_rust".to_string(),
                "--".to_string(),
                "--inputs".to_string(),
                paths.imagery_db.display().to_string(),
                paths.imagery_trim_db.display().to_string(),
                "--output".to_string(),
                paths.imagery_merge_db.display().to_string(),
                "--force".to_string(),
            ],
        },
        PipelineStepPlan {
            id: "9_step_move_sqls".to_string(),
            status: "wired".to_string(),
            description: "Move/copy final SQL artifacts into final folder".to_string(),
            command: vec![
                "cargo".to_string(),
                "run".to_string(),
                "-p".to_string(),
                "move_sqls_rust".to_string(),
                "--".to_string(),
                "--input-dir".to_string(),
                paths.imagery_dir.display().to_string(),
                "--output-dir".to_string(),
                paths.final_dir.display().to_string(),
                "--copy".to_string(),
                "--report".to_string(),
                paths
                    .final_dir
                    .join("move_sqls_report.json")
                    .display()
                    .to_string(),
            ],
        },
        PipelineStepPlan {
            id: "10_step_generative_fix".to_string(),
            status: "experimental".to_string(),
            description: "Generate missing-tile repair plan".to_string(),
            command: step10_cmd,
        },
    ];

    Ok(PipelinePlan {
        created_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        airports,
        jobs_file: paths.jobs_file.display().to_string(),
        download_manifest_file: paths.manifest_file.display().to_string(),
        steps,
    })
}

fn prepare_pipeline(args: &PrepareArgs) -> Result<PreparedRun> {
    fs::create_dir_all(&args.out_dir)?;
    let out_dir = args.out_dir.canonicalize().unwrap_or(args.out_dir.clone());
    let paths = derive_paths(&out_dir);

    let report = build_airport_index_report(
        &args.db,
        &args.icao,
        &default_cone_profiles(),
        args.three_by_three_radius,
        args.five_by_five_radius,
    )?;

    fs::write(&paths.jobs_file, serde_json::to_string_pretty(&report.jobs)?)?;
    let manifest = build_download_manifest(&report.jobs, &args.url_template);
    fs::write(&paths.manifest_file, serde_json::to_string_pretty(&manifest)?)?;
    let plan = build_step_plan(&paths, args)?;
    fs::write(&paths.plan_file, serde_json::to_string_pretty(&plan)?)?;

    fs::create_dir_all(&paths.state_dir)?;
    fs::create_dir_all(&paths.step_state_dir)?;

    if args.print_plan {
        println!("{}", serde_json::to_string_pretty(&plan)?);
    }

    println!("Pipeline prep complete.");
    println!("Airports requested: {:?}", report.airports_requested);
    println!("Airports found    : {:?}", report.airports_found);
    println!("Airports missing  : {:?}", report.airports_missing);
    println!("Cone jobs         : {}", report.jobs.len());
    println!("Tiles in manifest : {}", manifest.tile_count);
    println!("Jobs file         : {}", paths.jobs_file.display());
    println!("Manifest file     : {}", paths.manifest_file.display());
    println!("Plan file         : {}", paths.plan_file.display());

    Ok(PreparedRun {
        plan,
        paths,
        expected_job_count: report.jobs.len(),
        expected_tile_count: manifest.tile_count,
    })
}

fn run_prepare(args: PrepareArgs) -> Result<()> {
    let _ = prepare_pipeline(&args)?;
    Ok(())
}

fn run_id_from_airports(airports: &[String]) -> String {
    let airports = airports
        .iter()
        .map(|x| x.trim().to_uppercase())
        .collect::<Vec<_>>()
        .join("-");
    format!("{}-{airports}", now_unix_secs())
}

fn csv_escape(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn append_state_event(
    paths: &PipelinePaths,
    run_id: &str,
    airports: &[String],
    step_id: &str,
    event: &str,
    status: &str,
    input_count: Option<usize>,
    output_count: Option<usize>,
    message: &str,
) -> Result<()> {
    let file_exists = paths.state_csv.exists();
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.state_csv)
        .with_context(|| format!("Failed to open {}", paths.state_csv.display()))?;

    if !file_exists {
        writeln!(
            file,
            "timestamp_unix,run_id,airports,step_id,event,status,input_count,output_count,message"
        )?;
    }

    writeln!(
        file,
        "{},{},{},{},{},{},{},{},{}",
        now_unix_secs(),
        csv_escape(run_id),
        csv_escape(&airports.join("|")),
        csv_escape(step_id),
        csv_escape(event),
        csv_escape(status),
        input_count
            .map(|x| x.to_string())
            .unwrap_or_else(|| "".to_string()),
        output_count
            .map(|x| x.to_string())
            .unwrap_or_else(|| "".to_string()),
        csv_escape(message),
    )?;

    Ok(())
}

fn write_context_txt(
    paths: &PipelinePaths,
    run_id: &str,
    plan: &PipelinePlan,
    current_step: Option<&str>,
    completed_steps: &[String],
    skipped_steps: &[String],
    failed_step: Option<&str>,
    note: &str,
) -> Result<()> {
    let mut lines = Vec::new();
    lines.push(format!("run_id={run_id}"));
    lines.push(format!("updated_unix={}", now_unix_secs()));
    lines.push(format!("airports={}", plan.airports.join(",")));
    lines.push(format!("out_dir={}", paths.out_dir.display()));
    lines.push(format!("jobs_file={}", paths.jobs_file.display()));
    lines.push(format!("manifest_file={}", paths.manifest_file.display()));
    lines.push(format!("plan_file={}", paths.plan_file.display()));
    lines.push(format!("state_csv={}", paths.state_csv.display()));
    lines.push(format!(
        "current_step={}",
        current_step.unwrap_or("pipeline_complete")
    ));
    lines.push(format!("completed_steps={}", completed_steps.join(",")));
    lines.push(format!("skipped_steps={}", skipped_steps.join(",")));
    lines.push(format!("failed_step={}", failed_step.unwrap_or("")));
    lines.push(format!("note={note}"));
    lines.push(format!(
        "next_action={}",
        if failed_step.is_some() {
            "fix failure and rerun build"
        } else if completed_steps.len() + skipped_steps.len() >= plan.steps.len() {
            "pipeline finished"
        } else {
            "running"
        }
    ));
    fs::write(&paths.context_txt, format!("{}\n", lines.join("\n")))?;
    Ok(())
}

fn acquire_run_lock(paths: &PipelinePaths, args: &PrepareArgs, run_id: &str) -> Result<RunLock> {
    if paths.pipeline_lock_file.exists() {
        if args.force_unlock {
            fs::remove_file(&paths.pipeline_lock_file).with_context(|| {
                format!(
                    "Failed to remove stale lock {}",
                    paths.pipeline_lock_file.display()
                )
            })?;
        } else {
            bail!(
                "Lock file exists at {}. Another run may be active. Use --force-unlock if this is stale.",
                paths.pipeline_lock_file.display()
            );
        }
    }

    let mut lock = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&paths.pipeline_lock_file)
        .with_context(|| format!("Failed to create {}", paths.pipeline_lock_file.display()))?;
    writeln!(lock, "run_id={run_id}")?;
    writeln!(lock, "started_unix={}", now_unix_secs())?;

    Ok(RunLock {
        path: paths.pipeline_lock_file.clone(),
    })
}

fn step_sentinel_path(paths: &PipelinePaths, step_id: &str) -> PathBuf {
    paths.step_state_dir.join(format!("{step_id}.done.txt"))
}

fn write_step_sentinel(
    paths: &PipelinePaths,
    step: &PipelineStepPlan,
    run_id: &str,
    metrics: &StepMetrics,
) -> Result<()> {
    let sentinel_path = step_sentinel_path(paths, &step.id);
    let text = format!(
        "run_id={run_id}\nstep_id={}\ncompleted_unix={}\ninput_count={}\noutput_count={}\nnote={}\ncommand={}\n",
        step.id,
        now_unix_secs(),
        metrics
            .input_count
            .map(|x| x.to_string())
            .unwrap_or_else(|| "".to_string()),
        metrics
            .output_count
            .map(|x| x.to_string())
            .unwrap_or_else(|| "".to_string()),
        metrics.note,
        step.command.join(" ")
    );
    fs::write(sentinel_path, text)?;
    Ok(())
}

fn run_step_command(step: &PipelineStepPlan) -> Result<()> {
    let (program, args) = step
        .command
        .split_first()
        .ok_or_else(|| anyhow!("Step {} has an empty command vector", step.id))?;
    let status = ProcessCommand::new(program)
        .args(args)
        .status()
        .with_context(|| format!("Failed to launch step {}", step.id))?;
    if !status.success() {
        bail!("Step {} failed with status {status}", step.id);
    }
    Ok(())
}

fn count_files_matching(root: &Path, mut predicate: impl FnMut(&Path) -> bool) -> usize {
    if !root.exists() {
        return 0;
    }

    WalkDir::new(root)
        .follow_links(false)
        .into_iter()
        .filter_map(|entry| entry.ok())
        .filter(|entry| entry.file_type().is_file() && predicate(entry.path()))
        .count()
}

fn is_image_file(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "png" || ext == "jpg" || ext == "jpeg"
        })
        .unwrap_or(false)
}

fn is_tiff_file(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "tif" || ext == "tiff"
        })
        .unwrap_or(false)
}

fn is_vrt_file(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("vrt"))
        .unwrap_or(false)
}

fn is_geotiff_list_file(path: &Path) -> bool {
    if !path
        .extension()
        .and_then(|x| x.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("txt"))
        .unwrap_or(false)
    {
        return false;
    }
    path.file_stem()
        .and_then(|x| x.to_str())
        .map(|stem| stem.contains("_baseX") && stem.contains("_baseY"))
        .unwrap_or(false)
}

fn is_sql_artifact(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "sqlite" || ext == "db" || ext == "gpkg" || ext == "mbtiles"
        })
        .unwrap_or(false)
}

fn is_image_ext_pgw_duplicate(path: &Path) -> bool {
    let file_name = path
        .file_name()
        .and_then(|x| x.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    file_name.ends_with(".png.pgw")
        || file_name.ends_with(".jpg.pgw")
        || file_name.ends_with(".jpeg.pgw")
}

fn load_jobs(path: &Path) -> Result<Vec<ConeJob>> {
    let raw = fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let jobs = serde_json::from_str::<Vec<ConeJob>>(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(jobs)
}

fn load_manifest(path: &Path) -> Result<DownloadManifest> {
    let raw = fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    let manifest = serde_json::from_str::<DownloadManifest>(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))?;
    Ok(manifest)
}

fn validate_step_outputs(step_id: &str, prepared: &PreparedRun, args: &PrepareArgs) -> Result<StepMetrics> {
    let paths = &prepared.paths;
    let mut metrics = StepMetrics::default();

    match step_id {
        "1_step_airport_indexer" => {
            let jobs = load_jobs(&paths.jobs_file)?;
            if jobs.len() != prepared.expected_job_count {
                bail!(
                    "Step 1 produced {} jobs, expected {}",
                    jobs.len(),
                    prepared.expected_job_count
                );
            }
            metrics.output_count = Some(jobs.len());
            metrics.note = "Cone jobs refreshed".to_string();
        }
        "2_step_rust_downloader" => {
            let manifest = load_manifest(&paths.manifest_file)?;
            if manifest.tile_count != prepared.expected_tile_count {
                bail!(
                    "Step 2 manifest tile_count={} differs from prepared tile_count={}",
                    manifest.tile_count,
                    prepared.expected_tile_count
                );
            }
            metrics.output_count = Some(manifest.tile_count);
            if args.dry_run {
                metrics.note = "Dry-run: manifest generated, tiles not downloaded".to_string();
            } else {
                let raw_count = count_files_matching(&paths.raw_tiles_dir, is_image_file);
                if raw_count > manifest.tile_count {
                    bail!(
                        "Step 2 downloaded {raw_count} files, which is more than manifest tile_count={}",
                        manifest.tile_count
                    );
                }
                if raw_count < manifest.tile_count {
                    bail!(
                        "Step 2 downloaded {raw_count} files, expected {}. Use --resume to finish partial downloads.",
                        manifest.tile_count
                    );
                }
                metrics.input_count = Some(manifest.tile_count);
                metrics.output_count = Some(raw_count);
                metrics.note = "Raw tile download count matches manifest".to_string();
            }
        }
        "3_step_rust_brcj" => {
            let input_count = count_files_matching(&paths.raw_tiles_dir, is_image_file);
            let output_count = count_files_matching(&paths.brcj_dir, is_image_file);
            if output_count > input_count {
                bail!(
                    "Step 3 output image count ({output_count}) exceeds input image count ({input_count})"
                );
            }
            if output_count < input_count {
                bail!(
                    "Step 3 output image count ({output_count}) is lower than input image count ({input_count})"
                );
            }
            metrics.input_count = Some(input_count);
            metrics.output_count = Some(output_count);
            metrics.note = "BRCJ output count matches input count (no double-darkening)".to_string();
        }
        "4_step_pgw_rust" => {
            let image_count = count_files_matching(&paths.brcj_dir, is_image_file);
            let pgw_count = count_files_matching(&paths.brcj_dir, |p| {
                p.extension()
                    .and_then(|x| x.to_str())
                    .map(|ext| ext.eq_ignore_ascii_case("pgw"))
                    .unwrap_or(false)
                    && !is_image_ext_pgw_duplicate(p)
            });
            let duplicate_ext_pgw = count_files_matching(&paths.brcj_dir, is_image_ext_pgw_duplicate);
            if duplicate_ext_pgw > 0 {
                bail!(
                    "Step 4 created {duplicate_ext_pgw} duplicate *.png.pgw/*.jpg.pgw files. Keep --write-image-ext-pgw disabled."
                );
            }
            if pgw_count != image_count {
                bail!(
                    "Step 4 PGW count mismatch: pgw_count={pgw_count}, image_count={image_count}"
                );
            }
            metrics.input_count = Some(image_count);
            metrics.output_count = Some(pgw_count);
            metrics.note = "PGW sidecars aligned 1:1 with BRCJ tiles".to_string();
        }
        "5_step_geotiff_to_heaven" => {
            if args.dry_run {
                metrics.note = "Dry-run: GeoTIFF commands generated but not executed".to_string();
                return Ok(metrics);
            }

            let expected = prepared.expected_job_count;
            let tif_count = count_files_matching(&paths.geotiff_dir, is_tiff_file);
            let vrt_count = count_files_matching(&paths.geotiff_dir, is_vrt_file);
            let list_count = count_files_matching(&paths.geotiff_dir, is_geotiff_list_file);

            if tif_count > expected {
                bail!("Step 5 generated too many GeoTIFFs: {tif_count} > expected {expected}");
            }
            if tif_count < expected {
                bail!("Step 5 generated too few GeoTIFFs: {tif_count} < expected {expected}");
            }
            if vrt_count != expected {
                bail!("Step 5 VRT count mismatch: {vrt_count} != expected {expected}");
            }
            if list_count != expected {
                bail!("Step 5 list-file count mismatch: {list_count} != expected {expected}");
            }

            metrics.input_count = Some(expected);
            metrics.output_count = Some(tif_count);
            metrics.note = "GeoTIFF/VRT/list counts all match expected job count".to_string();
        }
        "6_step_rust_imagery_tiler" => {
            if args.dry_run {
                metrics.note = "Dry-run: imagery-tiler command rendered only".to_string();
                return Ok(metrics);
            }
            if !paths.imagery_db.exists() {
                bail!("Step 6 output DB missing: {}", paths.imagery_db.display());
            }
            let size = fs::metadata(&paths.imagery_db)?.len() as usize;
            if size == 0 {
                bail!("Step 6 output DB is empty: {}", paths.imagery_db.display());
            }
            metrics.output_count = Some(1);
            metrics.note = format!("Imagery DB created ({} bytes)", size);
        }
        "7_step_rust_adios_trim" => {
            if !paths.imagery_trim_db.exists() {
                bail!("Step 7 output missing: {}", paths.imagery_trim_db.display());
            }
            let size = fs::metadata(&paths.imagery_trim_db)?.len() as usize;
            if size == 0 {
                bail!("Step 7 output is empty: {}", paths.imagery_trim_db.display());
            }
            metrics.output_count = Some(1);
            metrics.note = format!("Trimmed DB ready ({} bytes)", size);
        }
        "8_step_merge_tiles" => {
            if !paths.imagery_merge_db.exists() {
                bail!("Step 8 output missing: {}", paths.imagery_merge_db.display());
            }
            let size = fs::metadata(&paths.imagery_merge_db)?.len() as usize;
            if size == 0 {
                bail!("Step 8 output is empty: {}", paths.imagery_merge_db.display());
            }
            metrics.output_count = Some(1);
            metrics.note = format!("Merged DB ready ({} bytes)", size);
        }
        "9_step_move_sqls" => {
            let expected_sql = count_files_matching(&paths.imagery_dir, is_sql_artifact);
            let final_sql = count_files_matching(&paths.final_dir, is_sql_artifact);
            if expected_sql == 0 {
                bail!(
                    "Step 9 source imagery dir has no SQL artifacts: {}",
                    paths.imagery_dir.display()
                );
            }
            if final_sql > expected_sql {
                bail!(
                    "Step 9 final SQL count too high: {final_sql} > expected {expected_sql} (duplicate move/copy suspected)"
                );
            }
            if final_sql < expected_sql {
                bail!(
                    "Step 9 final SQL count too low: {final_sql} < expected {expected_sql}"
                );
            }
            metrics.input_count = Some(expected_sql);
            metrics.output_count = Some(final_sql);
            metrics.note = "Final SQL artifact count matches imagery source count".to_string();
        }
        "10_step_generative_fix" => {
            if !paths.generative_plan.exists() {
                bail!(
                    "Step 10 output missing: {}",
                    paths.generative_plan.display()
                );
            }
            metrics.output_count = Some(1);
            metrics.note = "Repair plan file generated".to_string();
        }
        _ => {
            metrics.note = "No post-check rule configured".to_string();
        }
    }

    Ok(metrics)
}

fn execute_pipeline(prepared: &PreparedRun, args: &PrepareArgs) -> Result<()> {
    if args.distributed {
        eprintln!("[WARN] --distributed is not implemented yet; running local execution only.");
    }
    if !args.nodes.is_empty() {
        eprintln!(
            "[WARN] --nodes provided ({}), but distributed execution is not wired yet.",
            args.nodes.join(",")
        );
    }

    let run_id = run_id_from_airports(&prepared.plan.airports);
    let _lock = acquire_run_lock(&prepared.paths, args, &run_id)?;
    let mut completed_steps = Vec::new();
    let mut skipped_steps = Vec::new();

    append_state_event(
        &prepared.paths,
        &run_id,
        &prepared.plan.airports,
        "pipeline",
        "start",
        "running",
        None,
        None,
        "Pipeline build started",
    )?;
    write_context_txt(
        &prepared.paths,
        &run_id,
        &prepared.plan,
        Some("pipeline_start"),
        &completed_steps,
        &skipped_steps,
        None,
        "Pipeline run started",
    )?;

    for step in &prepared.plan.steps {
        if args.dry_run
            && matches!(
                step.id.as_str(),
                "3_step_rust_brcj"
                    | "4_step_pgw_rust"
                    | "5_step_geotiff_to_heaven"
                    | "6_step_rust_imagery_tiler"
                    | "7_step_rust_adios_trim"
                    | "8_step_merge_tiles"
                    | "9_step_move_sqls"
                    | "10_step_generative_fix"
            )
        {
            append_state_event(
                &prepared.paths,
                &run_id,
                &prepared.plan.airports,
                &step.id,
                "skip",
                "skipped_dry_run",
                None,
                None,
                "Global --dry-run is enabled; skipping non-prep execution stages",
            )?;
            skipped_steps.push(step.id.clone());
            write_context_txt(
                &prepared.paths,
                &run_id,
                &prepared.plan,
                Some(&step.id),
                &completed_steps,
                &skipped_steps,
                None,
                "Step skipped due to --dry-run",
            )?;
            continue;
        }

        let sentinel = step_sentinel_path(&prepared.paths, &step.id);
        if sentinel.exists() && !args.allow_rerun {
            let metrics = validate_step_outputs(&step.id, prepared, args).with_context(|| {
                format!(
                    "Step {} marked done, but output validation failed. Use --allow-rerun to rebuild this step.",
                    step.id
                )
            })?;
            append_state_event(
                &prepared.paths,
                &run_id,
                &prepared.plan.airports,
                &step.id,
                "skip",
                "skipped_done",
                metrics.input_count,
                metrics.output_count,
                &format!(
                    "Sentinel exists at {}; step skipped",
                    sentinel.display()
                ),
            )?;
            skipped_steps.push(step.id.clone());
            write_context_txt(
                &prepared.paths,
                &run_id,
                &prepared.plan,
                Some(&step.id),
                &completed_steps,
                &skipped_steps,
                None,
                "Step skipped (already completed)",
            )?;
            continue;
        }

        append_state_event(
            &prepared.paths,
            &run_id,
            &prepared.plan.airports,
            &step.id,
            "start",
            "running",
            None,
            None,
            &format!("Running: {}", step.command.join(" ")),
        )?;
        write_context_txt(
            &prepared.paths,
            &run_id,
            &prepared.plan,
            Some(&step.id),
            &completed_steps,
            &skipped_steps,
            None,
            "Step running",
        )?;

        if let Err(err) = run_step_command(step) {
            append_state_event(
                &prepared.paths,
                &run_id,
                &prepared.plan.airports,
                &step.id,
                "finish",
                "failed",
                None,
                None,
                &err.to_string(),
            )?;
            write_context_txt(
                &prepared.paths,
                &run_id,
                &prepared.plan,
                Some(&step.id),
                &completed_steps,
                &skipped_steps,
                Some(&step.id),
                &format!("Step failed: {err}"),
            )?;
            return Err(err);
        }

        let metrics = validate_step_outputs(&step.id, prepared, args)
            .with_context(|| format!("Step {} output validation failed", step.id))?;
        write_step_sentinel(&prepared.paths, step, &run_id, &metrics)?;
        append_state_event(
            &prepared.paths,
            &run_id,
            &prepared.plan.airports,
            &step.id,
            "finish",
            "completed",
            metrics.input_count,
            metrics.output_count,
            &metrics.note,
        )?;
        completed_steps.push(step.id.clone());
        write_context_txt(
            &prepared.paths,
            &run_id,
            &prepared.plan,
            Some(&step.id),
            &completed_steps,
            &skipped_steps,
            None,
            &metrics.note,
        )?;
    }

    append_state_event(
        &prepared.paths,
        &run_id,
        &prepared.plan.airports,
        "pipeline",
        "finish",
        "completed",
        None,
        None,
        "Pipeline build finished successfully",
    )?;
    write_context_txt(
        &prepared.paths,
        &run_id,
        &prepared.plan,
        None,
        &completed_steps,
        &skipped_steps,
        None,
        "Pipeline finished successfully",
    )?;

    println!("Pipeline build complete.");
    println!("Run ID           : {run_id}");
    println!("Completed steps  : {}", completed_steps.join(", "));
    if !skipped_steps.is_empty() {
        println!("Skipped steps    : {}", skipped_steps.join(", "));
    }
    println!("State CSV        : {}", prepared.paths.state_csv.display());
    println!("Context TXT      : {}", prepared.paths.context_txt.display());
    Ok(())
}

fn run_build(args: PrepareArgs) -> Result<()> {
    if !args.dry_run && args.url_template.contains("example.invalid") {
        bail!(
            "The default --url-template is a placeholder. Pass a real tile URL template for full build runs."
        );
    }

    let prepared = prepare_pipeline(&args)?;
    execute_pipeline(&prepared, &args)
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Prepare(args) => run_prepare(args),
        Command::Build(args) => run_build(args),
    }
}
