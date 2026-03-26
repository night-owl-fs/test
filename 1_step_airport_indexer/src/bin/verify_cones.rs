use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::PathBuf;

use anyhow::{anyhow, Context, Result};
use clap::Parser;
use pipeline_core::{build_airport_cone_to_heaven_report, render_cone_spec_text, ConeJob};

#[derive(Parser, Debug)]
#[command(name = "verify_cones")]
#[command(about = "Strictly verify generated Cone-to-Heaven blocks against master spec docs")]
struct Args {
    #[arg(
        long,
        default_value = "crates/1_step_airport_indexer/WW.ChartDataAOPA.sqlite"
    )]
    db: PathBuf,

    #[arg(
        long,
        default_value = "crates/1_step_airport_indexer/TILE_INDEX_MASTER_KEY_fixed_kmwl.txt"
    )]
    master_index_file: PathBuf,

    #[arg(long)]
    icao: Vec<String>,

    #[arg(long)]
    out_generated_specs_dir: Option<PathBuf>,

    #[arg(long)]
    report_json: Option<PathBuf>,

    #[arg(long)]
    allow_missing_in_master: bool,
}

#[derive(Debug, Clone)]
struct Block {
    base_z: u32,
    grid: u32,
    base_x: u32,
    base_y: u32,
    center_x: u32,
    center_y: u32,
    out_zooms: Vec<u32>,
}

fn normalize_icao(s: &str) -> String {
    s.trim().to_uppercase()
}

fn parse_nums(input: &str) -> Vec<u32> {
    input
        .split(|c: char| !c.is_ascii_digit())
        .filter(|p| !p.is_empty())
        .filter_map(|n| n.parse::<u32>().ok())
        .collect()
}

fn parse_coord_pairs(line: &str) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    let mut idx = 0usize;
    let bytes = line.as_bytes();
    while idx < bytes.len() {
        let Some(start_rel) = line[idx..].find('(') else {
            break;
        };
        let start = idx + start_rel + 1;
        let Some(end_rel) = line[start..].find(')') else {
            break;
        };
        let end = start + end_rel;
        let inner = &line[start..end];
        let nums = parse_nums(inner);
        if nums.len() >= 2 {
            out.push((nums[0], nums[1]));
        }
        idx = end + 1;
    }
    out
}

fn parse_header_block(line: &str) -> Option<(u32, Vec<u32>)> {
    if !line.starts_with('[') || !line.contains("ZOOM") {
        return None;
    }
    let Some(zoom_pos) = line.find("ZOOM") else {
        return None;
    };
    let header_tail = &line[zoom_pos + 4..];
    let nums = parse_nums(header_tail);
    if nums.is_empty() {
        return None;
    }
    let base_z = nums[0];
    let out_zooms = nums.into_iter().skip(1).collect::<Vec<_>>();
    Some((base_z, out_zooms))
}

fn parse_master_blocks(master_text: &str) -> BTreeMap<String, Vec<Block>> {
    let mut airport_to_blocks: BTreeMap<String, Vec<Block>> = BTreeMap::new();
    let lines = master_text.lines().collect::<Vec<_>>();
    let mut i = 0usize;
    let mut current_airport: Option<String> = None;

    while i < lines.len() {
        let line = lines[i].trim();

        if line.len() == 4
            && line.starts_with('K')
            && line
                .chars()
                .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
        {
            current_airport = Some(line.to_string());
            i += 1;
            continue;
        }

        if let Some((base_z, mut out_zooms)) = parse_header_block(line) {
            let Some(icao) = current_airport.clone() else {
                i += 1;
                continue;
            };

            let mut coords = Vec::<(u32, u32)>::new();
            let mut center = None::<(u32, u32)>;
            i += 1;
            while i < lines.len() {
                let row = lines[i].trim();
                if row.is_empty() {
                    i += 1;
                    continue;
                }
                if parse_header_block(row).is_some() {
                    break;
                }
                if row.len() == 4
                    && row.starts_with('K')
                    && row
                        .chars()
                        .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit())
                {
                    break;
                }
                if row.to_ascii_uppercase().starts_with("CENTER") {
                    if let Some((cx, cy)) = parse_coord_pairs(row).first().copied() {
                        center = Some((cx, cy));
                    }
                    i += 1;
                    continue;
                }
                coords.extend(parse_coord_pairs(row));
                i += 1;
            }

            if coords.is_empty() {
                continue;
            }

            let unique_x = coords.iter().map(|(x, _)| *x).collect::<BTreeSet<_>>();
            let unique_y = coords.iter().map(|(_, y)| *y).collect::<BTreeSet<_>>();
            let grid_x = unique_x.len() as u32;
            let grid_y = unique_y.len() as u32;
            let grid = grid_x.max(grid_y);
            let base_x = unique_x.iter().next().copied().unwrap_or(0);
            let base_y = unique_y.iter().next().copied().unwrap_or(0);
            let (center_x, center_y) = center.unwrap_or((
                base_x + grid.saturating_sub(1) / 2,
                base_y + grid.saturating_sub(1) / 2,
            ));

            out_zooms.sort_unstable();
            out_zooms.dedup();

            airport_to_blocks.entry(icao).or_default().push(Block {
                base_z,
                grid,
                base_x,
                base_y,
                center_x,
                center_y,
                out_zooms,
            });

            continue;
        }

        i += 1;
    }

    airport_to_blocks
}

fn generated_blocks_from_jobs(jobs: &[ConeJob]) -> Vec<Block> {
    let mut by_key: BTreeMap<(u32, u32, u32, u32), Vec<u32>> = BTreeMap::new();
    for j in jobs {
        by_key
            .entry((j.base_z, j.grid, j.base_x, j.base_y))
            .or_default()
            .push(j.out_z);
    }

    by_key
        .into_iter()
        .map(|((base_z, grid, base_x, base_y), mut out_zooms)| {
            out_zooms.sort_unstable();
            out_zooms.dedup();
            Block {
                base_z,
                grid,
                base_x,
                base_y,
                center_x: base_x + grid / 2,
                center_y: base_y + grid / 2,
                out_zooms,
            }
        })
        .collect()
}

fn block_key(block: &Block) -> (u32, u32, u32) {
    let signature = if block.out_zooms.contains(&17) {
        17
    } else if block.out_zooms.contains(&18) {
        18
    } else if block.out_zooms.contains(&16) {
        16
    } else {
        13
    };
    (block.base_z, block.grid, signature)
}

fn fmt_block(block: &Block) -> String {
    format!(
        "base_z={} grid={} base=({}, {}) center=({}, {}) out={:?}",
        block.base_z,
        block.grid,
        block.base_x,
        block.base_y,
        block.center_x,
        block.center_y,
        block.out_zooms
    )
}

fn main() -> Result<()> {
    let args = Args::parse();
    let master_text = fs::read_to_string(&args.master_index_file)
        .with_context(|| format!("Failed to read {}", args.master_index_file.display()))?;
    let master = parse_master_blocks(&master_text);

    let airport_list = if args.icao.is_empty() {
        master.keys().cloned().collect::<Vec<_>>()
    } else {
        args.icao
            .iter()
            .map(|x| normalize_icao(x))
            .collect::<Vec<_>>()
    };

    let report = build_airport_cone_to_heaven_report(&args.db, &airport_list)?;
    if !report.airports_missing.is_empty() {
        return Err(anyhow!(
            "ICAOs missing in DB: {:?}",
            report.airports_missing
        ));
    }

    if let Some(out_dir) = &args.out_generated_specs_dir {
        fs::create_dir_all(out_dir)?;
        for airport in &report.airports {
            let airport_jobs = report
                .jobs
                .iter()
                .filter(|j| j.icao == airport.icao)
                .cloned()
                .collect::<Vec<_>>();
            let txt = render_cone_spec_text(airport, &airport_jobs);
            let path = out_dir.join(format!("{}_spec.txt", airport.icao));
            fs::write(&path, txt)?;
        }
    }

    let mut mismatch_lines = Vec::new();
    let mut passed = 0usize;
    let mut checked = 0usize;
    let mut report_rows = Vec::new();

    for airport in &report.airports {
        checked += 1;
        let generated_jobs = report
            .jobs
            .iter()
            .filter(|j| j.icao == airport.icao)
            .cloned()
            .collect::<Vec<_>>();
        let gen_blocks = generated_blocks_from_jobs(&generated_jobs);
        let gen_by_key = gen_blocks
            .iter()
            .map(|b| (block_key(b), b.clone()))
            .collect::<BTreeMap<_, _>>();

        let Some(master_blocks) = master.get(&airport.icao) else {
            if args.allow_missing_in_master {
                report_rows.push(serde_json::json!({
                    "icao": airport.icao,
                    "status": "missing_in_master_allowed"
                }));
                continue;
            }
            mismatch_lines.push(format!(
                "{}: airport not found in master index file",
                airport.icao
            ));
            report_rows.push(serde_json::json!({
                "icao": airport.icao,
                "status": "missing_in_master"
            }));
            continue;
        };

        let master_by_key = master_blocks
            .iter()
            .map(|b| (block_key(b), b.clone()))
            .collect::<BTreeMap<_, _>>();

        let mut local_mismatch = Vec::new();
        for (k, gen_block) in &gen_by_key {
            let Some(master_block) = master_by_key.get(k) else {
                local_mismatch.push(format!(
                    "missing master block for key {:?} generated={}",
                    k,
                    fmt_block(gen_block)
                ));
                continue;
            };

            if gen_block.base_x != master_block.base_x
                || gen_block.base_y != master_block.base_y
                || gen_block.grid != master_block.grid
                || gen_block.center_x != master_block.center_x
                || gen_block.center_y != master_block.center_y
                || gen_block.out_zooms != master_block.out_zooms
            {
                local_mismatch.push(format!(
                    "key {:?} generated={} master={}",
                    k,
                    fmt_block(gen_block),
                    fmt_block(master_block)
                ));
            }
        }

        for k in master_by_key.keys() {
            if !gen_by_key.contains_key(k) {
                local_mismatch.push(format!("unexpected master block key {:?}", k));
            }
        }

        if local_mismatch.is_empty() {
            println!("[PASS] {}", airport.icao);
            passed += 1;
            report_rows.push(serde_json::json!({
                "icao": airport.icao,
                "status": "pass"
            }));
        } else {
            println!("[FAIL] {}", airport.icao);
            for line in &local_mismatch {
                println!("  - {line}");
            }
            mismatch_lines.push(format!("{}: {}", airport.icao, local_mismatch.join(" | ")));
            report_rows.push(serde_json::json!({
                "icao": airport.icao,
                "status": "fail",
                "mismatches": local_mismatch
            }));
        }
    }

    println!(
        "Verification summary: checked={} passed={} failed={}",
        checked,
        passed,
        checked.saturating_sub(passed)
    );

    if let Some(path) = &args.report_json {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let payload = serde_json::json!({
            "checked": checked,
            "passed": passed,
            "failed": checked.saturating_sub(passed),
            "rows": report_rows,
        });
        fs::write(path, serde_json::to_string_pretty(&payload)?)?;
        println!("Wrote verification report: {}", path.display());
    }

    if mismatch_lines.is_empty() {
        Ok(())
    } else {
        Err(anyhow!(
            "Cone verification failed for {} airport(s): {}",
            mismatch_lines.len(),
            mismatch_lines.join("; ")
        ))
    }
}
