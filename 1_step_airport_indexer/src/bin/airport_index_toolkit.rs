use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Parser, Subcommand};
use regex::Regex;
use serde::{Deserialize, Serialize};

const WEB_MERCATOR_HALF: f64 = 20_037_508.342_789_244;
const WEB_MERCATOR_WORLD: f64 = WEB_MERCATOR_HALF * 2.0;
const TILE_SIZE: u32 = 256;
const DEFAULT_INDEX_FILE: &str =
    "crates/1_step_airport_indexer/TILE_INDEX_MASTER_KEY_fixed_kmwl.txt";
const AIRPORT_FOLDER_ALIASES: &[(&str, &str)] = &[("KCRQ", "KRCQ")];

#[derive(Parser, Debug)]
#[command(name = "airport_index_toolkit")]
#[command(about = "Rust-first replacement for legacy step 1 airport Python tooling")]
struct Cli {
    #[command(subcommand)]
    command: ToolkitCommand,
}

#[derive(Subcommand, Debug)]
enum ToolkitCommand {
    GenerateSpecs(GenerateSpecsArgs),
    BuildGeotiffs(BuildGeotiffsArgs),
    BuildFromIndex(BuildFromIndexArgs),
}

#[derive(Args, Debug, Clone)]
struct GenerateSpecsArgs {
    #[arg(long, default_value = ".")]
    root: PathBuf,

    #[arg(long, default_value = DEFAULT_INDEX_FILE)]
    index_file: PathBuf,

    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long, default_value = "index")]
    source: String,

    #[arg(long)]
    manual_specs_json: Option<PathBuf>,

    #[arg(long = "airport")]
    airports: Vec<String>,
}

#[derive(Args, Debug, Clone)]
struct BuildGeotiffsArgs {
    #[arg(long, default_value = ".")]
    root: PathBuf,

    #[arg(long)]
    specs_dir: PathBuf,

    #[arg(long)]
    out_dir: PathBuf,

    #[arg(long)]
    gdal_bin: Option<PathBuf>,

    #[arg(long = "airport")]
    airports: Vec<String>,
}

#[derive(Args, Debug, Clone)]
struct BuildFromIndexArgs {
    #[command(flatten)]
    generate: GenerateSpecsArgs,

    #[arg(long)]
    geotiff_out_dir: PathBuf,

    #[arg(long)]
    gdal_bin: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MinimalSpec {
    airport: String,
    tile_zoom: u32,
    output_zoom: u32,
    base_zoom: u32,
    tile_size: u32,
    groups: Vec<MinimalSpecGroup>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MinimalSpecGroup {
    name: String,
    files: Vec<String>,
    output_tiff: String,
}

#[derive(Debug, Clone)]
struct IndexBlock {
    base_zoom: u32,
    output_zooms: Vec<u32>,
    x_min: u32,
    x_max: u32,
    y_min: u32,
    y_max: u32,
}

#[derive(Debug, Clone, Deserialize)]
struct ManualSpecPayload {
    src_zoom: BTreeMap<String, u32>,
    specs_map: BTreeMap<String, BTreeMap<String, Vec<Vec<[u32; 2]>>>>,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        ToolkitCommand::GenerateSpecs(args) => generate_specs(args),
        ToolkitCommand::BuildGeotiffs(args) => build_geotiffs(args),
        ToolkitCommand::BuildFromIndex(args) => build_from_index(args),
    }
}

fn build_from_index(args: BuildFromIndexArgs) -> Result<()> {
    generate_specs(args.generate.clone())?;
    build_geotiffs(BuildGeotiffsArgs {
        root: args.generate.root,
        specs_dir: args.generate.out_dir,
        out_dir: args.geotiff_out_dir,
        gdal_bin: args.gdal_bin,
        airports: args.generate.airports,
    })
}

fn generate_specs(args: GenerateSpecsArgs) -> Result<()> {
    fs::create_dir_all(&args.out_dir)?;
    let airport_filter = normalize_airport_filter(&args.airports);
    let source = args.source.trim().to_lowercase();
    let specs = match source.as_str() {
        "index" => {
            let index_file = resolve_relative(&args.root, &args.index_file);
            generate_specs_from_index(&args.root, &index_file, &airport_filter)?
        }
        "manual" => {
            let manual_path = args.manual_specs_json.clone().unwrap_or_else(|| {
                args.root
                    .join("crates/1_step_airport_indexer/manual_specs.json")
            });
            generate_specs_from_manual(&manual_path, &airport_filter)?
        }
        other => bail!("Unsupported source '{other}'. Use 'index' or 'manual'."),
    };

    let mut written = 0usize;
    for spec in specs {
        let file_name = format!("{}_Z{}_MINIMAL_spec.json", spec.airport, spec.output_zoom);
        let out_path = args.out_dir.join(file_name);
        fs::write(&out_path, serde_json::to_string_pretty(&spec)?)?;
        println!("Wrote {}", out_path.display());
        written += 1;
    }

    if written == 0 {
        bail!("No spec files were generated.");
    }

    println!("Generated {written} spec file(s) from {source} source.");
    Ok(())
}

fn build_geotiffs(args: BuildGeotiffsArgs) -> Result<()> {
    let airport_filter = normalize_airport_filter(&args.airports);
    let specs_dir = resolve_relative(&args.root, &args.specs_dir);
    let out_root = resolve_relative(&args.root, &args.out_dir);
    fs::create_dir_all(&out_root)?;
    let gdal_buildvrt = resolve_gdal_tool(args.gdal_bin.as_deref(), "gdalbuildvrt")?;
    let gdal_translate = resolve_gdal_tool(args.gdal_bin.as_deref(), "gdal_translate")?;

    let mut built = 0usize;
    let mut warnings = Vec::new();
    for entry in
        fs::read_dir(&specs_dir).with_context(|| format!("Reading {}", specs_dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|value| value.to_str()) != Some("json") {
            continue;
        }

        let spec = load_spec(&path)?;
        if !airport_filter.is_empty() && !airport_filter.contains(&spec.airport) {
            continue;
        }

        let airport_root = resolve_airport_dir(&args.root, &spec.airport);
        let tile_dir = airport_root.join(spec.tile_zoom.to_string());
        if !tile_dir.exists() {
            warnings.push(format!(
                "{} Z{}: tile folder missing ({})",
                spec.airport,
                spec.tile_zoom,
                tile_dir.display()
            ));
            continue;
        }

        let airport_out = out_root
            .join(&spec.airport)
            .join(format!("Z{}", spec.output_zoom));
        fs::create_dir_all(&airport_out)?;

        for group in &spec.groups {
            let tiles = collect_group_tiles(&tile_dir, spec.tile_zoom, &group.files)?;
            if tiles.is_empty() {
                warnings.push(format!(
                    "{} {}: no tiles resolved from {}",
                    spec.airport,
                    group.name,
                    path.display()
                ));
                continue;
            }

            let vrt_path = airport_out.join(format!("{}.vrt", group.name));
            let tif_path = airport_out.join(&group.output_tiff);
            build_geotiff_group(
                &gdal_buildvrt,
                &gdal_translate,
                &tiles,
                &vrt_path,
                &tif_path,
            )?;
            built += 1;
        }
    }

    println!("Built {built} GeoTIFF group(s).");
    if !warnings.is_empty() {
        eprintln!("Warnings:");
        for warning in warnings {
            eprintln!("- {warning}");
        }
    }

    if built == 0 {
        bail!("No GeoTIFF groups were built.");
    }

    Ok(())
}

fn generate_specs_from_index(
    root: &Path,
    index_file: &Path,
    airport_filter: &BTreeSet<String>,
) -> Result<Vec<MinimalSpec>> {
    let text = fs::read_to_string(index_file)
        .with_context(|| format!("Failed to read {}", index_file.display()))?;
    let sections = parse_tile_index(&text)?;
    let mut specs_by_key: BTreeMap<(String, u32), MinimalSpec> = BTreeMap::new();

    for (airport, blocks) in sections {
        if !airport_filter.is_empty() && !airport_filter.contains(&airport) {
            continue;
        }

        let airport_root = resolve_airport_dir(root, &airport);
        for block in blocks {
            for output_zoom in &block.output_zooms {
                let key = (airport.clone(), *output_zoom);
                let spec = specs_by_key.entry(key).or_insert_with(|| MinimalSpec {
                    airport: airport.clone(),
                    tile_zoom: *output_zoom,
                    output_zoom: *output_zoom,
                    base_zoom: block.base_zoom,
                    tile_size: TILE_SIZE,
                    groups: Vec::new(),
                });

                for (row_index, base_y) in (block.y_min..=block.y_max).enumerate() {
                    for (column_index, base_x) in (block.x_min..=block.x_max).enumerate() {
                        let files = expand_base_tile_to_files(
                            *output_zoom,
                            block.base_zoom,
                            base_x,
                            base_y,
                        );
                        if files.is_empty() {
                            continue;
                        }
                        let name = format!(
                            "Z{output_zoom}_fromZ{}_r{row_index}_c{column_index}_baseX{base_x}_baseY{base_y}",
                            block.base_zoom
                        );
                        spec.groups.push(MinimalSpecGroup {
                            output_tiff: format!("{name}.tif"),
                            name,
                            files,
                        });
                    }
                }

                let tile_dir = airport_root.join(output_zoom.to_string());
                if !tile_dir.exists() {
                    eprintln!(
                        "[WARN] {} Z{} tile folder missing during spec generation: {}",
                        airport,
                        output_zoom,
                        tile_dir.display()
                    );
                }
            }
        }
    }

    Ok(specs_by_key
        .into_values()
        .filter(|spec| !spec.groups.is_empty())
        .collect())
}

fn generate_specs_from_manual(
    manual_specs_json: &Path,
    airport_filter: &BTreeSet<String>,
) -> Result<Vec<MinimalSpec>> {
    let payload = serde_json::from_str::<ManualSpecPayload>(
        &fs::read_to_string(manual_specs_json)
            .with_context(|| format!("Failed to read {}", manual_specs_json.display()))?,
    )?;

    let mut specs = Vec::new();
    for (airport, zoom_map) in payload.specs_map {
        if !airport_filter.is_empty() && !airport_filter.contains(&airport) {
            continue;
        }

        let tile_zoom = payload.src_zoom.get(&airport).copied().unwrap_or(18);
        for (zoom_label, groups) in zoom_map {
            let output_zoom = zoom_label
                .trim_start_matches('Z')
                .parse::<u32>()
                .with_context(|| format!("Invalid manual zoom label '{zoom_label}'"))?;
            let merc_zoom = mercator_zoom_for_output_zoom(output_zoom);
            let mut spec = MinimalSpec {
                airport: airport.clone(),
                tile_zoom,
                output_zoom,
                base_zoom: merc_zoom,
                tile_size: TILE_SIZE,
                groups: Vec::new(),
            };

            for (group_index, group) in groups.into_iter().enumerate() {
                let coords = group
                    .into_iter()
                    .map(|pair| (pair[0], pair[1]))
                    .collect::<Vec<_>>();
                let files = expand_coords_to_files(&coords, tile_zoom, merc_zoom);
                let name = format!("{}_Z{}_MANUAL_G{}", airport, output_zoom, group_index + 1);
                spec.groups.push(MinimalSpecGroup {
                    name: name.clone(),
                    output_tiff: format!("{name}.tif"),
                    files,
                });
            }
            specs.push(spec);
        }
    }

    Ok(specs)
}

fn resolve_relative(root: &Path, value: &Path) -> PathBuf {
    if value.is_absolute() {
        value.to_path_buf()
    } else {
        root.join(value)
    }
}

fn normalize_airport_filter(airports: &[String]) -> BTreeSet<String> {
    airports
        .iter()
        .map(|airport| airport.trim().to_uppercase())
        .filter(|airport| !airport.is_empty())
        .collect()
}

fn resolve_airport_dir(root: &Path, airport: &str) -> PathBuf {
    let direct = root.join(airport);
    if direct.exists() {
        return direct;
    }

    for (source, alias) in AIRPORT_FOLDER_ALIASES {
        if airport == *source {
            let candidate = root.join(alias);
            if candidate.exists() {
                return candidate;
            }
        }
    }

    direct
}

fn mercator_zoom_for_output_zoom(output_zoom: u32) -> u32 {
    match output_zoom {
        13 | 14 | 15 => 9,
        16 => 11,
        17 | 18 => 13,
        other => other,
    }
}

fn expand_base_tile_to_files(
    output_zoom: u32,
    base_zoom: u32,
    base_x: u32,
    base_y: u32,
) -> Vec<String> {
    let factor = if output_zoom >= base_zoom {
        1u32 << (output_zoom - base_zoom)
    } else {
        0
    };
    if factor == 0 {
        return Vec::new();
    }

    let x0 = base_x * factor;
    let y0 = base_y * factor;
    let mut files = Vec::with_capacity((factor * factor) as usize);
    for y in y0..(y0 + factor) {
        for x in x0..(x0 + factor) {
            files.push(format!("{x}_{y}.png"));
        }
    }
    files
}

fn expand_coords_to_files(coords: &[(u32, u32)], tile_zoom: u32, merc_zoom: u32) -> Vec<String> {
    let factor = if tile_zoom >= merc_zoom {
        1u32 << (tile_zoom - merc_zoom)
    } else {
        0
    };
    let mut files = BTreeSet::new();
    for (merc_x, merc_y) in coords {
        let start_x = merc_x * factor;
        let start_y = merc_y * factor;
        for x in start_x..(start_x + factor) {
            for y in start_y..(start_y + factor) {
                files.insert(format!("{x}_{y}.png"));
            }
        }
    }
    files.into_iter().collect()
}

fn parse_tile_index(text: &str) -> Result<BTreeMap<String, Vec<IndexBlock>>> {
    let section_re = Regex::new(r"^\s*(K[A-Z0-9]{3})\s*$")?;
    let header_re = Regex::new(r"\[ZOOM\s*(\d+)[^\]]*?\]")?;
    let out_zoom_re = Regex::new(r"Z(\d+)")?;
    let coord_re = Regex::new(r"\(\s*(\d+)\s*[,\.]\s*(\d+)\s*\)")?;

    let lines = text.lines().collect::<Vec<_>>();
    let mut airport = None::<String>;
    let mut index = 0usize;
    let mut sections: BTreeMap<String, Vec<IndexBlock>> = BTreeMap::new();

    while index < lines.len() {
        let line = lines[index].trim();
        if let Some(captures) = section_re.captures(line) {
            airport = Some(captures[1].to_string());
            index += 1;
            continue;
        }

        if let Some(current_airport) = airport.clone() {
            if let Some(header) = header_re.captures(line) {
                let base_zoom = header[1].parse::<u32>()?;
                let output_zooms = out_zoom_re
                    .captures_iter(line)
                    .filter_map(|capture| capture[1].parse::<u32>().ok())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect::<Vec<_>>();
                let mut coords = Vec::new();
                let mut cursor = index + 1;
                while cursor < lines.len() {
                    let row = lines[cursor].trim();
                    if row.is_empty() || row.to_ascii_uppercase().starts_with("CENTER") {
                        cursor += 1;
                        continue;
                    }
                    if row.starts_with("[ZOOM") || section_re.is_match(row) {
                        break;
                    }
                    for capture in coord_re.captures_iter(row) {
                        coords.push((capture[1].parse::<u32>()?, capture[2].parse::<u32>()?));
                    }
                    cursor += 1;
                }

                if !coords.is_empty() && !output_zooms.is_empty() {
                    let (x_min, x_max) = coords
                        .iter()
                        .map(|(x, _)| *x)
                        .fold((u32::MAX, 0u32), |(min_value, max_value), x| {
                            (min_value.min(x), max_value.max(x))
                        });
                    let (y_min, y_max) = coords
                        .iter()
                        .map(|(_, y)| *y)
                        .fold((u32::MAX, 0u32), |(min_value, max_value), y| {
                            (min_value.min(y), max_value.max(y))
                        });

                    sections
                        .entry(current_airport)
                        .or_default()
                        .push(IndexBlock {
                            base_zoom,
                            output_zooms,
                            x_min,
                            x_max,
                            y_min,
                            y_max,
                        });
                }

                index = cursor;
                continue;
            }
        }

        index += 1;
    }

    Ok(sections)
}

fn load_spec(path: &Path) -> Result<MinimalSpec> {
    let raw =
        fs::read_to_string(path).with_context(|| format!("Failed to read {}", path.display()))?;
    serde_json::from_str::<MinimalSpec>(&raw)
        .with_context(|| format!("Failed to parse {}", path.display()))
}

fn collect_group_tiles(tile_dir: &Path, tile_zoom: u32, files: &[String]) -> Result<Vec<PathBuf>> {
    let mut tiles = Vec::new();
    let mut seen = BTreeSet::new();
    for file in files {
        if !seen.insert(file.clone()) {
            continue;
        }
        let tile_path = tile_dir.join(file);
        if tile_path.exists() {
            ensure_worldfile(&tile_path, tile_zoom)?;
            tiles.push(tile_path.canonicalize().unwrap_or(tile_path));
        }
    }
    Ok(tiles)
}

fn ensure_worldfile(tile_png: &Path, zoom: u32) -> Result<()> {
    let pgw_path = tile_png.with_extension("pgw");
    if pgw_path.exists() {
        return Ok(());
    }

    let png_pgw_path = PathBuf::from(format!("{}.pgw", tile_png.display()));
    if png_pgw_path.exists() {
        fs::copy(&png_pgw_path, &pgw_path)?;
        return Ok(());
    }

    let stem = tile_png
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("Invalid tile filename: {}", tile_png.display()))?;
    let mut parts = stem.split('_');
    let x = parts
        .next()
        .ok_or_else(|| anyhow!("Missing x coordinate in {}", tile_png.display()))?
        .parse::<u32>()?;
    let y = parts
        .next()
        .ok_or_else(|| anyhow!("Missing y coordinate in {}", tile_png.display()))?
        .parse::<u32>()?;

    let n = 1u32 << zoom;
    let resolution = WEB_MERCATOR_WORLD / (n as f64 * TILE_SIZE as f64);
    let min_x = -WEB_MERCATOR_HALF + x as f64 * TILE_SIZE as f64 * resolution;
    let max_y = WEB_MERCATOR_HALF - y as f64 * TILE_SIZE as f64 * resolution;
    let center_x = min_x + resolution / 2.0;
    let center_y = max_y - resolution / 2.0;

    let content = format!(
        "{resolution:.12}\n0.000000000000\n0.000000000000\n-{resolution:.12}\n{center_x:.12}\n{center_y:.12}\n"
    );
    fs::write(pgw_path, content)?;
    Ok(())
}

fn build_geotiff_group(
    gdal_buildvrt: &Path,
    gdal_translate: &Path,
    tiles: &[PathBuf],
    vrt_path: &Path,
    tif_path: &Path,
) -> Result<()> {
    let list_path = vrt_path.with_extension("txt");
    let list_body = tiles
        .iter()
        .map(|tile| format!("{}\n", tile.display()))
        .collect::<String>();
    fs::write(&list_path, list_body)?;

    let build_result = run_command(
        gdal_buildvrt,
        &[
            "-input_file_list",
            list_path.to_string_lossy().as_ref(),
            vrt_path.to_string_lossy().as_ref(),
        ],
    );
    let translate_result = build_result.and_then(|_| {
        run_command(
            gdal_translate,
            &[
                "-of",
                "GTiff",
                "-a_srs",
                "EPSG:3857",
                "-co",
                "TILED=YES",
                "-co",
                "COMPRESS=DEFLATE",
                "-co",
                "PREDICTOR=2",
                "-co",
                "ZLEVEL=6",
                "-co",
                "BIGTIFF=IF_SAFER",
                vrt_path.to_string_lossy().as_ref(),
                tif_path.to_string_lossy().as_ref(),
            ],
        )
    });

    let _ = fs::remove_file(&list_path);
    translate_result
}

fn resolve_gdal_tool(gdal_bin: Option<&Path>, base_name: &str) -> Result<PathBuf> {
    let executable_name = if cfg!(windows) {
        format!("{base_name}.exe")
    } else {
        base_name.to_string()
    };
    let mut candidates = Vec::new();
    if let Some(bin_dir) = gdal_bin {
        candidates.push(bin_dir.join(&executable_name));
    }
    candidates.push(PathBuf::from(format!(
        "/mnt/c/OSGeo4W/bin/{executable_name}"
    )));
    candidates.push(PathBuf::from(format!("C:/OSGeo4W/bin/{executable_name}")));

    for candidate in &candidates {
        if candidate.exists() {
            return Ok(candidate.clone());
        }
    }

    if gdal_bin.is_none() {
        return Ok(PathBuf::from(executable_name));
    }

    bail!("Unable to resolve GDAL tool '{}'", executable_name)
}

fn run_command(command: &Path, args: &[&str]) -> Result<()> {
    let output = Command::new(command)
        .args(args)
        .output()
        .with_context(|| format!("Failed to start {}", command.display()))?;
    if output.status.success() {
        return Ok(());
    }

    bail!(
        "Command failed: {} {}\n{}",
        command.display(),
        args.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
}
