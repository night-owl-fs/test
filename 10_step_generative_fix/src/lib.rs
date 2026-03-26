mod upsample;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use image::imageops::{self, FilterType};
use image::{DynamicImage, Pixel, Rgb, RgbImage};
use script_tools_rust::{match_library_image, LibraryMatch};
use serde::Serialize;
use std::process::Command;
use upsample::descendant_from_ancestor;
use walkdir::WalkDir;

pub const TILE_SIZE: u32 = 256;
const WEBM_HALF: f64 = 20037508.342789244;
const WEBM_WORLD: f64 = WEBM_HALF * 2.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct TileCoord {
    pub z: u32,
    pub x: u32,
    pub y: u32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SourceTileRef {
    pub namespace: String,
    pub z: u32,
    pub x: u32,
    pub y: u32,
    pub path: String,
}

#[derive(Debug, Clone, Copy, Serialize)]
pub struct ImageMetrics {
    pub mean_luma: f32,
    pub std_luma: f32,
    pub mean_r: f32,
    pub mean_g: f32,
    pub mean_b: f32,
    pub dynamic_range: f32,
}

#[derive(Debug, Clone, Serialize)]
pub struct SuperFillConfig {
    pub repair_bad_tiles: bool,
    pub tiny_bytes: u64,
    pub dark_mean_threshold: f32,
    pub dark_std_threshold: f32,
    pub flat_std_threshold: f32,
    pub flat_mean_ceiling: f32,
    pub placeholder_hashes: Vec<String>,
    pub neighbor_radius: u32,
    pub large_gap_radius: u32,
    pub enable_context_tuning: bool,
    pub water_blue_gain: f32,
    pub water_green_gain: f32,
    pub greenery_green_gain: f32,
    pub greenery_blue_gain: f32,
    pub global_saturation: f32,
    pub global_contrast: f32,
    pub patch_library: Option<String>,
    pub enable_patch_matching: bool,
    pub patch_topk: u32,
    pub write_worldfiles: bool,
    pub seam_aware_blend: bool,
    pub seam_feather_px: u32,
    pub seam_neighbor_weight: f32,
}

impl Default for SuperFillConfig {
    fn default() -> Self {
        Self {
            repair_bad_tiles: true,
            tiny_bytes: 3_500,
            dark_mean_threshold: 5.0,
            dark_std_threshold: 3.0,
            flat_std_threshold: 0.9,
            flat_mean_ceiling: 75.0,
            placeholder_hashes: vec!["f27d9de7f80c13501f470595e327aa6d".to_string()],
            neighbor_radius: 10,
            large_gap_radius: 24,
            enable_context_tuning: true,
            water_blue_gain: 1.12,
            water_green_gain: 0.92,
            greenery_green_gain: 1.12,
            greenery_blue_gain: 0.92,
            global_saturation: 1.04,
            global_contrast: 1.03,
            patch_library: None,
            enable_patch_matching: false,
            patch_topk: 1,
            write_worldfiles: true,
            seam_aware_blend: false,
            seam_feather_px: 16,
            seam_neighbor_weight: 1.0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MissingTile {
    pub namespace: String,
    pub z: u32,
    pub x: u32,
    pub y: u32,
    pub parent_hint: Option<(u32, u32, u32)>,
    pub target_path: String,
    pub original_state: String,
    pub chosen_method: Option<String>,
    pub context: String,
    pub source_tiles: Vec<SourceTileRef>,
    pub attempted_methods: Vec<String>,
    pub quality_before: Option<ImageMetrics>,
    pub quality_after: Option<ImageMetrics>,
    pub output_size_bytes: Option<u64>,
    pub note: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct RepairPlan {
    pub missing_count: usize,
    pub generated_count: usize,
    pub written_count: usize,
    pub unresolved_count: usize,
    pub method_counts: BTreeMap<String, usize>,
    pub config: SuperFillConfig,
    pub summary: String,
    pub super_doc: String,
    pub missing: Vec<MissingTile>,
}

#[derive(Debug, Clone)]
pub struct TileInventory {
    pub root: PathBuf,
    pub by_namespace: BTreeMap<String, BTreeMap<u32, BTreeMap<(u32, u32), PathBuf>>>,
}

#[derive(Debug, Clone)]
struct TileHealth {
    good: bool,
    reason: String,
    metrics: Option<ImageMetrics>,
    #[allow(dead_code)]
    size_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ContextKind {
    Neutral,
    Water,
    Greenery,
}

impl ContextKind {
    fn as_str(self) -> &'static str {
        match self {
            ContextKind::Neutral => "neutral",
            ContextKind::Water => "water",
            ContextKind::Greenery => "greenery",
        }
    }
}

#[derive(Debug, Clone)]
struct Candidate {
    method: String,
    image: RgbImage,
    metrics: ImageMetrics,
    source_tiles: Vec<SourceTileRef>,
    score: f32,
}

#[derive(Debug, Clone)]
struct TargetTile {
    namespace: String,
    coord: TileCoord,
    original_path: Option<PathBuf>,
    original_state: String,
    quality_before: Option<ImageMetrics>,
    note: Option<String>,
}

pub fn discover_xyz_tiles(root: &Path) -> BTreeMap<u32, BTreeSet<(u32, u32)>> {
    let mut out = BTreeMap::<u32, BTreeSet<(u32, u32)>>::new();
    for (_namespace, by_zoom) in discover_tile_inventory(root).by_namespace {
        for (z, coords) in by_zoom {
            out.entry(z)
                .or_default()
                .extend(coords.into_keys().collect::<BTreeSet<_>>());
        }
    }
    out
}

pub fn discover_tile_inventory(root: &Path) -> TileInventory {
    let mut by_namespace: BTreeMap<String, BTreeMap<u32, BTreeMap<(u32, u32), PathBuf>>> =
        BTreeMap::new();

    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if !is_tile_extension(path) {
            continue;
        }
        let Some((namespace, z, x, y)) = parse_namespaced_xyz_path(root, path) else {
            continue;
        };
        by_namespace
            .entry(namespace)
            .or_default()
            .entry(z)
            .or_default()
            .insert((x, y), path.to_path_buf());
    }

    TileInventory {
        root: root.to_path_buf(),
        by_namespace,
    }
}

fn is_tile_extension(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| {
            ext.eq_ignore_ascii_case("png")
                || ext.eq_ignore_ascii_case("jpg")
                || ext.eq_ignore_ascii_case("jpeg")
        })
        .unwrap_or(false)
}

fn parse_namespaced_xyz_path(root: &Path, path: &Path) -> Option<(String, u32, u32, u32)> {
    let y = path
        .file_stem()
        .and_then(|x| x.to_str())
        .and_then(|s| s.parse::<u32>().ok())?;

    let x_dir = path.parent()?;
    let x = x_dir
        .file_name()
        .and_then(|x| x.to_str())
        .and_then(|s| s.parse::<u32>().ok())?;

    let z_dir = x_dir.parent()?;
    let z = z_dir
        .file_name()
        .and_then(|x| x.to_str())
        .and_then(|s| s.parse::<u32>().ok())?;

    let namespace_path = z_dir.parent()?.strip_prefix(root).ok()?;
    let namespace = namespace_path.to_string_lossy().replace('\\', "/");
    Some((namespace, z, x, y))
}

fn namespace_matches(namespace: &str, filters: &[String]) -> bool {
    if filters.is_empty() {
        return true;
    }
    let ns_lower = namespace.to_ascii_lowercase();
    let tail = ns_lower
        .split('/')
        .next_back()
        .map(ToOwned::to_owned)
        .unwrap_or_default();

    filters.iter().any(|f| {
        let f = f.trim().to_ascii_lowercase();
        ns_lower == f || tail == f
    })
}

pub fn build_super_repair_plan(
    root: &Path,
    zooms: &[u32],
    airports: &[String],
    max_missing: usize,
    apply: bool,
    config: SuperFillConfig,
) -> Result<RepairPlan> {
    let mut filler = SuperFiller::new(root, config.clone());

    let namespaces = filler
        .inventory
        .by_namespace
        .keys()
        .filter(|ns| namespace_matches(ns, airports))
        .cloned()
        .collect::<Vec<_>>();

    let mut targets = Vec::new();
    for namespace in namespaces {
        let requested = if zooms.is_empty() {
            filler
                .inventory
                .by_namespace
                .get(&namespace)
                .map(|m| m.keys().copied().collect::<Vec<_>>())
                .unwrap_or_default()
        } else {
            zooms.to_vec()
        };

        for z in requested {
            let Some((min_x, max_x, min_y, max_y)) = (if let Some(coords) = filler
                .inventory
                .by_namespace
                .get(&namespace)
                .and_then(|m| m.get(&z))
            {
                if coords.is_empty() {
                    None
                } else {
                    Some((
                        coords.keys().map(|(x, _)| *x).min().unwrap_or(0),
                        coords.keys().map(|(x, _)| *x).max().unwrap_or(0),
                        coords.keys().map(|(_, y)| *y).min().unwrap_or(0),
                        coords.keys().map(|(_, y)| *y).max().unwrap_or(0),
                    ))
                }
            } else {
                None
            }) else {
                continue;
            };

            for x in min_x..=max_x {
                for y in min_y..=max_y {
                    if targets.len() >= max_missing {
                        break;
                    }

                    let coord = TileCoord { z, x, y };
                    let maybe_path = filler.coord_path(&namespace, z, x, y);
                    let parent_hint = if z > 0 {
                        Some((z - 1, x / 2, y / 2))
                    } else {
                        None
                    };

                    if let Some(path) = maybe_path {
                        let health = filler.path_health(&path);
                        if health.good || !filler.config.repair_bad_tiles {
                            continue;
                        }
                        targets.push((
                            parent_hint,
                            TargetTile {
                                namespace: namespace.clone(),
                                coord,
                                original_path: Some(path),
                                original_state: "bad_tile".to_string(),
                                quality_before: health.metrics,
                                note: Some(health.reason),
                            },
                        ));
                    } else {
                        targets.push((
                            parent_hint,
                            TargetTile {
                                namespace: namespace.clone(),
                                coord,
                                original_path: None,
                                original_state: "missing".to_string(),
                                quality_before: None,
                                note: None,
                            },
                        ));
                    }
                }
                if targets.len() >= max_missing {
                    break;
                }
            }
            if targets.len() >= max_missing {
                break;
            }
        }
        if targets.len() >= max_missing {
            break;
        }
    }

    let mut missing = Vec::with_capacity(targets.len());
    let mut method_counts: BTreeMap<String, usize> = BTreeMap::new();
    let mut generated_count = 0usize;
    let mut written_count = 0usize;
    let mut unresolved_count = 0usize;

    for (parent_hint, target) in targets {
        let (entry, was_generated, was_written) =
            filler.process_target(target, parent_hint, apply)?;
        if was_generated {
            generated_count += 1;
            if let Some(name) = &entry.chosen_method {
                *method_counts.entry(name.clone()).or_insert(0) += 1;
            }
        } else {
            unresolved_count += 1;
            *method_counts.entry("unresolved".to_string()).or_insert(0) += 1;
        }
        if was_written {
            written_count += 1;
        }
        missing.push(entry);
    }

    let summary = format!(
        "targets={} generated={} written={} unresolved={} apply_mode={} namespaces={}",
        missing.len(),
        generated_count,
        written_count,
        unresolved_count,
        apply,
        if airports.is_empty() {
            "ALL".to_string()
        } else {
            airports.join(",")
        }
    );

    Ok(RepairPlan {
        missing_count: missing.len(),
        generated_count,
        written_count,
        unresolved_count,
        method_counts,
        config,
        summary,
        super_doc: "crates/10_step_generative_fix/SUPER_GENERATIVE_FILL_RUST_DOC.md".to_string(),
        missing,
    })
}

struct SuperFiller {
    root: PathBuf,
    inventory: TileInventory,
    config: SuperFillConfig,
    health_cache: HashMap<PathBuf, TileHealth>,
    image_cache: HashMap<PathBuf, RgbImage>,
    global_color_cache: HashMap<(String, u32), (f32, f32, f32)>,
}

impl SuperFiller {
    fn new(root: &Path, config: SuperFillConfig) -> Self {
        Self {
            root: root.to_path_buf(),
            inventory: discover_tile_inventory(root),
            config,
            health_cache: HashMap::new(),
            image_cache: HashMap::new(),
            global_color_cache: HashMap::new(),
        }
    }

    fn coord_path(&self, namespace: &str, z: u32, x: u32, y: u32) -> Option<PathBuf> {
        self.inventory
            .by_namespace
            .get(namespace)
            .and_then(|m| m.get(&z))
            .and_then(|m| m.get(&(x, y)).cloned())
    }

    fn default_output_path(&self, namespace: &str, z: u32, x: u32, y: u32) -> PathBuf {
        let mut out = self.root.clone();
        if !namespace.is_empty() {
            out = out.join(namespace);
        }
        out.join(z.to_string())
            .join(x.to_string())
            .join(format!("{y}.png"))
    }

    fn preferred_output_path(
        &self,
        namespace: &str,
        coord: TileCoord,
        original_path: Option<&Path>,
    ) -> PathBuf {
        if let Some(path) = original_path {
            if path
                .extension()
                .and_then(|e| e.to_str())
                .map(|e| e.eq_ignore_ascii_case("png"))
                .unwrap_or(false)
            {
                return path.to_path_buf();
            }
            if let Some(parent) = path.parent() {
                return parent.join(format!("{}.png", coord.y));
            }
        }

        if let Some(by_zoom) = self.inventory.by_namespace.get(namespace) {
            if let Some(by_coord) = by_zoom.get(&coord.z) {
                if let Some((_, sample)) = by_coord.iter().find(|((sx, _), _)| *sx == coord.x) {
                    if let Some(parent) = sample.parent() {
                        return parent.join(format!("{}.png", coord.y));
                    }
                }
                if let Some(sample) = by_coord.values().next() {
                    if let Some(z_dir) = sample.parent().and_then(|p| p.parent()) {
                        return z_dir
                            .join(coord.x.to_string())
                            .join(format!("{}.png", coord.y));
                    }
                }
            }
        }

        self.default_output_path(namespace, coord.z, coord.x, coord.y)
    }

    fn path_health(&mut self, path: &Path) -> TileHealth {
        if let Some(h) = self.health_cache.get(path) {
            return h.clone();
        }

        let result = (|| {
            let meta = fs::metadata(path).ok()?;
            let size = meta.len();
            if size <= self.config.tiny_bytes {
                return Some(TileHealth {
                    good: false,
                    reason: format!("tiny_file({size}b)"),
                    metrics: None,
                    size_bytes: size,
                });
            }

            let img = self.load_rgb(path)?;
            let metrics = compute_image_metrics(&img);

            if !self.config.placeholder_hashes.is_empty() {
                if let Ok(bytes) = fs::read(path) {
                    let hash = format!("{:x}", md5::compute(bytes));
                    if self
                        .config
                        .placeholder_hashes
                        .iter()
                        .any(|h| h.eq_ignore_ascii_case(&hash))
                    {
                        return Some(TileHealth {
                            good: false,
                            reason: format!("placeholder_hash({hash})"),
                            metrics: Some(metrics),
                            size_bytes: size,
                        });
                    }
                }
            }

            if metrics.mean_luma < self.config.dark_mean_threshold
                && metrics.std_luma < self.config.dark_std_threshold
            {
                return Some(TileHealth {
                    good: false,
                    reason: "flat_dark".to_string(),
                    metrics: Some(metrics),
                    size_bytes: size,
                });
            }

            if metrics.std_luma < self.config.flat_std_threshold
                && metrics.mean_luma < self.config.flat_mean_ceiling
            {
                return Some(TileHealth {
                    good: false,
                    reason: "flat_low_variance".to_string(),
                    metrics: Some(metrics),
                    size_bytes: size,
                });
            }

            Some(TileHealth {
                good: true,
                reason: "good".to_string(),
                metrics: Some(metrics),
                size_bytes: size,
            })
        })();

        let health = result.unwrap_or(TileHealth {
            good: false,
            reason: "decode_error".to_string(),
            metrics: None,
            size_bytes: 0,
        });
        self.health_cache.insert(path.to_path_buf(), health.clone());
        health
    }

    fn load_rgb(&mut self, path: &Path) -> Option<RgbImage> {
        if let Some(img) = self.image_cache.get(path) {
            return Some(img.clone());
        }

        let dyn_img = image::open(path).ok()?;
        let rgb = dyn_img.to_rgb8();
        let normalized = if rgb.width() == TILE_SIZE && rgb.height() == TILE_SIZE {
            rgb
        } else {
            imageops::resize(&rgb, TILE_SIZE, TILE_SIZE, FilterType::Lanczos3)
        };
        self.image_cache
            .insert(path.to_path_buf(), normalized.clone());
        Some(normalized)
    }

    fn load_good_tile(
        &mut self,
        namespace: &str,
        z: u32,
        x: u32,
        y: u32,
    ) -> Option<(PathBuf, RgbImage, ImageMetrics)> {
        let path = self.coord_path(namespace, z, x, y)?;
        let health = self.path_health(&path);
        if !health.good {
            return None;
        }
        let img = self.load_rgb(&path)?;
        let metrics = health.metrics?;
        Some((path, img, metrics))
    }

    fn rel_path(&self, path: &Path) -> String {
        path.strip_prefix(&self.root)
            .ok()
            .map(|p| p.to_string_lossy().replace('\\', "/"))
            .unwrap_or_else(|| path.to_string_lossy().replace('\\', "/"))
    }

    fn source_ref(&self, namespace: &str, z: u32, x: u32, y: u32, path: &Path) -> SourceTileRef {
        SourceTileRef {
            namespace: namespace.to_string(),
            z,
            x,
            y,
            path: self.rel_path(path),
        }
    }

    fn process_target(
        &mut self,
        target: TargetTile,
        parent_hint: Option<(u32, u32, u32)>,
        apply: bool,
    ) -> Result<(MissingTile, bool, bool)> {
        let namespace = target.namespace.clone();
        let coord = target.coord;
        let context = self.classify_context(&namespace, coord.z, coord.x, coord.y);

        let mut attempted = Vec::new();
        let mut candidates = Vec::new();

        attempted.push("child_downsample".to_string());
        let child = self.method_child_downsample(&namespace, coord, context);
        if let Some(c) = child.clone() {
            candidates.push(c);
        }

        attempted.push("parent_upsample".to_string());
        let parent = self.method_parent_upsample(&namespace, coord, context);
        if let Some(c) = parent.clone() {
            candidates.push(c);
        }

        attempted.push("adjacent_zoom_blend".to_string());
        if let (Some(c1), Some(c2)) = (&child, &parent) {
            let blended = blend_images(&c1.image, &c2.image, 0.5);
            candidates.push(self.finalize_candidate(
                "adjacent_zoom_blend",
                blended,
                [c1.source_tiles.clone(), c2.source_tiles.clone()].concat(),
                28.0,
                0.0,
                context,
            ));
        }

        attempted.push("multi_zoom_synthesis".to_string());
        if let Some(c) = self.method_multi_zoom_best(&namespace, coord, context) {
            candidates.push(c);
        }

        attempted.push("neighbor_clone".to_string());
        if let Some(c) =
            self.method_neighbor_clone(&namespace, coord, target.quality_before, context)
        {
            candidates.push(c);
        }

        attempted.push("neighbor_blend".to_string());
        if let Some(c) = self.method_neighbor_blend(&namespace, coord, context) {
            candidates.push(c);
        }

        attempted.push("large_gap_idw".to_string());
        if let Some(c) = self.method_large_gap_idw(&namespace, coord, context) {
            candidates.push(c);
        }

        attempted.push("patch_library".to_string());
        if self.config.enable_patch_matching {
            if let Some(c) = self.method_patch_library(&namespace, coord, context) {
                candidates.push(c);
            }
        }

        attempted.push("solid_context_fill".to_string());
        if let Some(c) = self.method_solid_context_fill(&namespace, coord, context) {
            candidates.push(c);
        }

        let mut chosen = candidates.into_iter().max_by(|a, b| {
            a.score
                .partial_cmp(&b.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let planned_out_path =
            self.preferred_output_path(&namespace, coord, target.original_path.as_deref());
        let target_path = self.rel_path(&planned_out_path);

        if let Some(mut candidate) = chosen.take() {
            if self.config.seam_aware_blend {
                self.apply_seam_blend(&namespace, coord, &mut candidate.image);
                candidate.metrics = compute_image_metrics(&candidate.image);
                candidate.score = quality_score(candidate.metrics);
            }

            let mut written = false;
            let mut output_size = None;
            if apply {
                let out_path = planned_out_path;
                if let Some(parent) = out_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                DynamicImage::ImageRgb8(candidate.image.clone())
                    .save_with_format(&out_path, image::ImageFormat::Png)?;
                if self.config.write_worldfiles {
                    write_worldfiles_png(&out_path, coord.z, coord.x, coord.y)?;
                }
                output_size = Some(fs::metadata(&out_path)?.len());
                written = true;

                self.inventory
                    .by_namespace
                    .entry(namespace.clone())
                    .or_default()
                    .entry(coord.z)
                    .or_default()
                    .insert((coord.x, coord.y), out_path.clone());
                self.image_cache
                    .insert(out_path.clone(), candidate.image.clone());
                self.health_cache.insert(
                    out_path,
                    TileHealth {
                        good: true,
                        reason: "generated".to_string(),
                        metrics: Some(candidate.metrics),
                        size_bytes: output_size.unwrap_or(0),
                    },
                );
            }

            let entry = MissingTile {
                namespace,
                z: coord.z,
                x: coord.x,
                y: coord.y,
                parent_hint,
                target_path,
                original_state: target.original_state,
                chosen_method: Some(candidate.method),
                context: context.as_str().to_string(),
                source_tiles: truncate_sources(candidate.source_tiles),
                attempted_methods: attempted,
                quality_before: target.quality_before,
                quality_after: Some(candidate.metrics),
                output_size_bytes: output_size,
                note: if written {
                    target.note
                } else {
                    Some("planned_only(use --apply to write tiles)".to_string())
                },
            };
            return Ok((entry, true, written));
        }

        let unresolved = MissingTile {
            namespace,
            z: coord.z,
            x: coord.x,
            y: coord.y,
            parent_hint,
            target_path,
            original_state: target.original_state,
            chosen_method: None,
            context: context.as_str().to_string(),
            source_tiles: Vec::new(),
            attempted_methods: attempted,
            quality_before: target.quality_before,
            quality_after: None,
            output_size_bytes: None,
            note: Some(
                target
                    .note
                    .unwrap_or_else(|| "no_viable_sources".to_string()),
            ),
        };
        Ok((unresolved, false, false))
    }

    fn finalize_candidate(
        &self,
        method: &str,
        mut image: RgbImage,
        source_tiles: Vec<SourceTileRef>,
        method_bonus: f32,
        penalty: f32,
        context: ContextKind,
    ) -> Candidate {
        if self.config.enable_context_tuning {
            apply_context_tuning(&mut image, context, &self.config);
        }
        let metrics = compute_image_metrics(&image);
        let score = quality_score(metrics) + method_bonus - penalty;
        Candidate {
            method: method.to_string(),
            image,
            metrics,
            source_tiles,
            score,
        }
    }

    fn method_child_downsample(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let z = coord.z.checked_add(1)?;
        let children = [
            (coord.x.checked_mul(2)?, coord.y.checked_mul(2)?),
            (
                coord.x.checked_mul(2)?.checked_add(1)?,
                coord.y.checked_mul(2)?,
            ),
            (
                coord.x.checked_mul(2)?,
                coord.y.checked_mul(2)?.checked_add(1)?,
            ),
            (
                coord.x.checked_mul(2)?.checked_add(1)?,
                coord.y.checked_mul(2)?.checked_add(1)?,
            ),
        ];

        let mut imgs = Vec::with_capacity(4);
        let mut srcs = Vec::with_capacity(4);
        for (cx, cy) in children {
            let (path, img, _) = self.load_good_tile(namespace, z, cx, cy)?;
            imgs.push(img);
            srcs.push(self.source_ref(namespace, z, cx, cy, &path));
        }

        let mut mosaic = RgbImage::new(TILE_SIZE * 2, TILE_SIZE * 2);
        paste(&mut mosaic, &imgs[0], 0, 0);
        paste(&mut mosaic, &imgs[1], TILE_SIZE, 0);
        paste(&mut mosaic, &imgs[2], 0, TILE_SIZE);
        paste(&mut mosaic, &imgs[3], TILE_SIZE, TILE_SIZE);
        let out = imageops::resize(&mosaic, TILE_SIZE, TILE_SIZE, FilterType::Lanczos3);
        Some(self.finalize_candidate("child_downsample", out, srcs, 30.0, 0.0, context))
    }

    fn method_parent_upsample(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        if coord.z == 0 {
            return None;
        }
        let pz = coord.z - 1;
        let px = coord.x / 2;
        let py = coord.y / 2;
        let (path, parent_img, _) = self.load_good_tile(namespace, pz, px, py)?;
        let out = descendant_from_ancestor(
            &parent_img,
            TileCoord {
                z: pz,
                x: px,
                y: py,
            },
            coord,
            FilterType::CatmullRom,
        )?;
        Some(self.finalize_candidate(
            "parent_upsample",
            out,
            vec![self.source_ref(namespace, pz, px, py, &path)],
            20.0,
            0.0,
            context,
        ))
    }

    fn method_multi_zoom_best(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let z = coord.z;
        let mut source_zooms = vec![
            z.saturating_add(1),
            z.saturating_add(2),
            z.saturating_add(3),
            z.saturating_add(4),
        ];
        if z > 0 {
            source_zooms.push(z - 1);
        }
        if z > 1 {
            source_zooms.push(z - 2);
        }
        if z > 2 {
            source_zooms.push(z - 3);
        }
        source_zooms.retain(|src| *src != z && *src <= 30);
        source_zooms.sort_unstable();
        source_zooms.dedup();

        let mut best: Option<Candidate> = None;
        for src_z in source_zooms {
            let Some((img, srcs)) = self.synthesize_from_zoom(namespace, coord, src_z) else {
                continue;
            };
            let dist = src_z.abs_diff(z) as f32;
            let bonus = 24.0 - dist * 1.5;
            let cand = self.finalize_candidate(
                &format!("multi_zoom_synthesis_z{src_z}"),
                img,
                srcs,
                bonus,
                0.0,
                context,
            );
            if best.as_ref().map(|b| cand.score > b.score).unwrap_or(true) {
                best = Some(cand);
            }
        }
        best
    }

    fn synthesize_from_zoom(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        src_z: u32,
    ) -> Option<(RgbImage, Vec<SourceTileRef>)> {
        if src_z == coord.z {
            return None;
        }

        if src_z > coord.z {
            let delta = src_z - coord.z;
            if delta > 4 {
                return None;
            }
            let scale = 1u32 << delta;
            let mut mosaic = RgbImage::new(TILE_SIZE * scale, TILE_SIZE * scale);
            let mut srcs = Vec::with_capacity((scale * scale) as usize);
            for sy in 0..scale {
                for sx in 0..scale {
                    let tx = coord.x.checked_mul(scale)?.checked_add(sx)?;
                    let ty = coord.y.checked_mul(scale)?.checked_add(sy)?;
                    let (path, img, _) = self.load_good_tile(namespace, src_z, tx, ty)?;
                    paste(&mut mosaic, &img, sx * TILE_SIZE, sy * TILE_SIZE);
                    srcs.push(self.source_ref(namespace, src_z, tx, ty, &path));
                }
            }
            let out = imageops::resize(&mosaic, TILE_SIZE, TILE_SIZE, FilterType::Lanczos3);
            return Some((out, srcs));
        }

        let delta = coord.z - src_z;
        if delta > 8 {
            return None;
        }
        let ax = coord.x >> delta;
        let ay = coord.y >> delta;
        let (path, ancestor, _) = self.load_good_tile(namespace, src_z, ax, ay)?;
        let out = descendant_from_ancestor(
            &ancestor,
            TileCoord {
                z: src_z,
                x: ax,
                y: ay,
            },
            coord,
            FilterType::CatmullRom,
        )?;
        Some((out, vec![self.source_ref(namespace, src_z, ax, ay, &path)]))
    }

    fn method_neighbor_clone(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        preferred: Option<ImageMetrics>,
        context: ContextKind,
    ) -> Option<Candidate> {
        let mut best: Option<Candidate> = None;
        let mut best_score = f32::MIN;

        for r in 1..=self.config.neighbor_radius {
            let mut found_ring = false;
            for dy in -(r as i32)..=(r as i32) {
                for dx in -(r as i32)..=(r as i32) {
                    if dx == 0 && dy == 0 {
                        continue;
                    }
                    if dx.unsigned_abs().max(dy.unsigned_abs()) != r {
                        continue;
                    }
                    let nx = i64::from(coord.x) + i64::from(dx);
                    let ny = i64::from(coord.y) + i64::from(dy);
                    if nx < 0 || ny < 0 {
                        continue;
                    }
                    let nx = nx as u32;
                    let ny = ny as u32;
                    let Some((path, img, metrics)) =
                        self.load_good_tile(namespace, coord.z, nx, ny)
                    else {
                        continue;
                    };
                    found_ring = true;

                    let mut penalty = (r as f32) * 2.2;
                    if let Some(pref) = preferred {
                        penalty += (metrics.mean_luma - pref.mean_luma).abs() * 0.35;
                        penalty += (metrics.std_luma - pref.std_luma).abs() * 0.45;
                    }

                    let cand = self.finalize_candidate(
                        "neighbor_clone",
                        img,
                        vec![self.source_ref(namespace, coord.z, nx, ny, &path)],
                        16.0,
                        penalty,
                        context,
                    );
                    if cand.score > best_score {
                        best_score = cand.score;
                        best = Some(cand);
                    }
                }
            }
            if found_ring {
                break;
            }
        }

        best
    }

    fn method_neighbor_blend(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let mut picks: Vec<(u32, u32, u32, PathBuf, RgbImage)> = Vec::new();
        for r in 1..=self.config.neighbor_radius {
            for dy in -(r as i32)..=(r as i32) {
                for dx in -(r as i32)..=(r as i32) {
                    if dx == 0 && dy == 0 {
                        continue;
                    }
                    if dx.unsigned_abs() + dy.unsigned_abs() != r {
                        continue;
                    }
                    let nx = i64::from(coord.x) + i64::from(dx);
                    let ny = i64::from(coord.y) + i64::from(dy);
                    if nx < 0 || ny < 0 {
                        continue;
                    }
                    let nx = nx as u32;
                    let ny = ny as u32;
                    if let Some((path, img, _)) = self.load_good_tile(namespace, coord.z, nx, ny) {
                        picks.push((r, nx, ny, path, img));
                    }
                }
            }
            if picks.len() >= 4 {
                break;
            }
        }

        if picks.is_empty() {
            return None;
        }

        picks.sort_by_key(|(r, _, _, _, _)| *r);
        picks.truncate(4);
        let mut blend = picks[0].4.clone();
        for (idx, (_, _, _, _, img)) in picks.iter().enumerate().skip(1) {
            let alpha = 1.0 / (idx as f32 + 1.0);
            blend = blend_images(&blend, img, alpha);
        }

        let avg_dist = picks.iter().map(|(r, _, _, _, _)| *r as f32).sum::<f32>()
            / (picks.len() as f32).max(1.0);
        let srcs = picks
            .iter()
            .map(|(_, x, y, path, _)| self.source_ref(namespace, coord.z, *x, *y, path))
            .collect::<Vec<_>>();

        Some(self.finalize_candidate("neighbor_blend", blend, srcs, 14.0, avg_dist, context))
    }

    fn method_large_gap_idw(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let mut picks: Vec<(u32, u32, u32, PathBuf, RgbImage)> = Vec::new();
        for r in 1..=self.config.large_gap_radius {
            for dy in -(r as i32)..=(r as i32) {
                for dx in -(r as i32)..=(r as i32) {
                    if dx == 0 && dy == 0 {
                        continue;
                    }
                    if dx.unsigned_abs().max(dy.unsigned_abs()) != r {
                        continue;
                    }
                    let nx = i64::from(coord.x) + i64::from(dx);
                    let ny = i64::from(coord.y) + i64::from(dy);
                    if nx < 0 || ny < 0 {
                        continue;
                    }
                    let nx = nx as u32;
                    let ny = ny as u32;
                    if let Some((path, img, _)) = self.load_good_tile(namespace, coord.z, nx, ny) {
                        picks.push((r, nx, ny, path, img));
                    }
                }
            }
            if picks.len() >= 8 {
                break;
            }
        }

        if picks.len() < 3 {
            return None;
        }

        picks.sort_by_key(|(r, _, _, _, _)| *r);
        picks.truncate(8);

        let mut out = RgbImage::new(TILE_SIZE, TILE_SIZE);
        let weights = picks
            .iter()
            .map(|(r, _, _, _, _)| {
                let d = (*r as f32).max(1.0);
                1.0 / (d * d)
            })
            .collect::<Vec<_>>();
        let weight_sum = weights.iter().sum::<f32>().max(1e-6);

        for y in 0..TILE_SIZE {
            for x in 0..TILE_SIZE {
                let mut rr = 0.0f32;
                let mut gg = 0.0f32;
                let mut bb = 0.0f32;
                for ((_, _, _, _, img), w) in picks.iter().zip(weights.iter()) {
                    let px = img.get_pixel(x, y).channels();
                    rr += px[0] as f32 * *w;
                    gg += px[1] as f32 * *w;
                    bb += px[2] as f32 * *w;
                }
                out.put_pixel(
                    x,
                    y,
                    Rgb([
                        clamp_u8(rr / weight_sum),
                        clamp_u8(gg / weight_sum),
                        clamp_u8(bb / weight_sum),
                    ]),
                );
            }
        }

        let srcs = picks
            .iter()
            .map(|(_, x, y, path, _)| self.source_ref(namespace, coord.z, *x, *y, path))
            .collect::<Vec<_>>();

        Some(self.finalize_candidate("large_gap_idw", out, srcs, 8.0, 0.0, context))
    }

    fn method_solid_context_fill(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let local = self.local_context_color(namespace, coord.z, coord.x, coord.y, 12, 16);
        let global = self.global_zoom_color(namespace, coord.z);
        let (r, g, b) = local.or(global)?;

        let mut out = RgbImage::new(TILE_SIZE, TILE_SIZE);
        for y in 0..TILE_SIZE {
            for x in 0..TILE_SIZE {
                out.put_pixel(x, y, Rgb([clamp_u8(r), clamp_u8(g), clamp_u8(b)]));
            }
        }

        Some(self.finalize_candidate("solid_context_fill", out, Vec::new(), 4.0, 0.0, context))
    }

    fn method_patch_library(
        &mut self,
        namespace: &str,
        coord: TileCoord,
        context: ContextKind,
    ) -> Option<Candidate> {
        let lib = match &self.config.patch_library {
            Some(p) => p.clone(),
            None => return None,
        };

        let (r, g, b) = self
            .local_context_color(namespace, coord.z, coord.x, coord.y, 8, 12)
            .or_else(|| self.global_zoom_color(namespace, coord.z))
            .unwrap_or((128.0f32, 128.0f32, 128.0f32));

        let mut sample = RgbImage::new(128, 128);
        for y in 0..128 {
            for x in 0..128 {
                sample.put_pixel(x, y, Rgb([clamp_u8(r), clamp_u8(g), clamp_u8(b)]));
            }
        }
        let sample_dyn = DynamicImage::ImageRgb8(sample.clone());
        let matches = match match_library_image(
            &sample_dyn,
            Path::new(&lib),
            self.config.patch_topk as usize,
        ) {
            Ok(value) => value,
            Err(rust_error) => {
                eprintln!(
                    "[generative_fix_rust] Rust patch matcher failed: {rust_error}. Falling back to Python matcher."
                );
                let tmp_name = format!(
                    "patch_target_{}_{}_{}_{}.png",
                    namespace.replace('/', "_"),
                    coord.z,
                    coord.x,
                    coord.y
                );
                let tmp_path = std::env::temp_dir().join(tmp_name);
                if sample_dyn
                    .save_with_format(&tmp_path, image::ImageFormat::Png)
                    .is_err()
                {
                    return None;
                }

                let topk = self.config.patch_topk.to_string();
                let output = match Command::new("python3")
                    .arg("scripts/library_matcher.py")
                    .arg("--python-only")
                    .arg("--target")
                    .arg(&tmp_path)
                    .arg("--library")
                    .arg(&lib)
                    .arg("--topk")
                    .arg(&topk)
                    .output()
                {
                    Ok(value) if value.status.success() => value,
                    _ => {
                        let _ = std::fs::remove_file(&tmp_path);
                        return None;
                    }
                };

                let parsed = serde_json::from_slice::<Vec<LibraryMatch>>(&output.stdout).ok();
                let _ = std::fs::remove_file(&tmp_path);
                parsed?
            }
        };

        let first_path = matches.first()?.path.as_str();

        let dyn_img = image::open(first_path).ok()?;
        let rgb = dyn_img.to_rgb8();
        let resized = imageops::resize(&rgb, TILE_SIZE, TILE_SIZE, FilterType::Lanczos3);

        Some(self.finalize_candidate("patch_library", resized, Vec::new(), 6.0, 0.0, context))
    }

    fn classify_context(&mut self, namespace: &str, z: u32, x: u32, y: u32) -> ContextKind {
        let Some((r, g, b)) = self.local_context_color(namespace, z, x, y, 8, 12) else {
            return ContextKind::Neutral;
        };

        if b > g * 1.08 && b > r * 1.08 {
            return ContextKind::Water;
        }
        if g > r * 1.08 && g > b * 1.05 {
            return ContextKind::Greenery;
        }
        ContextKind::Neutral
    }

    fn local_context_color(
        &mut self,
        namespace: &str,
        z: u32,
        x: u32,
        y: u32,
        radius: u32,
        max_samples: usize,
    ) -> Option<(f32, f32, f32)> {
        let mut sum_r = 0.0f32;
        let mut sum_g = 0.0f32;
        let mut sum_b = 0.0f32;
        let mut count = 0usize;

        for r in 1..=radius {
            for dy in -(r as i32)..=(r as i32) {
                for dx in -(r as i32)..=(r as i32) {
                    if dx == 0 && dy == 0 {
                        continue;
                    }
                    if dx.unsigned_abs().max(dy.unsigned_abs()) != r {
                        continue;
                    }
                    let nx = i64::from(x) + i64::from(dx);
                    let ny = i64::from(y) + i64::from(dy);
                    if nx < 0 || ny < 0 {
                        continue;
                    }
                    let nx = nx as u32;
                    let ny = ny as u32;
                    let Some((_, _, metrics)) = self.load_good_tile(namespace, z, nx, ny) else {
                        continue;
                    };
                    sum_r += metrics.mean_r;
                    sum_g += metrics.mean_g;
                    sum_b += metrics.mean_b;
                    count += 1;
                    if count >= max_samples {
                        break;
                    }
                }
                if count >= max_samples {
                    break;
                }
            }
            if count >= max_samples {
                break;
            }
        }

        if count == 0 {
            None
        } else {
            Some((
                sum_r / count as f32,
                sum_g / count as f32,
                sum_b / count as f32,
            ))
        }
    }

    fn global_zoom_color(&mut self, namespace: &str, z: u32) -> Option<(f32, f32, f32)> {
        let key = (namespace.to_string(), z);
        if let Some(v) = self.global_color_cache.get(&key) {
            return Some(*v);
        }

        let sample_coords = self
            .inventory
            .by_namespace
            .get(namespace)?
            .get(&z)?
            .keys()
            .take(240)
            .copied()
            .collect::<Vec<_>>();

        let mut sum_r = 0.0f32;
        let mut sum_g = 0.0f32;
        let mut sum_b = 0.0f32;
        let mut count = 0usize;

        for (x, y) in sample_coords {
            if let Some((_, _, m)) = self.load_good_tile(namespace, z, x, y) {
                sum_r += m.mean_r;
                sum_g += m.mean_g;
                sum_b += m.mean_b;
                count += 1;
            }
        }

        if count == 0 {
            return None;
        }

        let avg = (
            sum_r / count as f32,
            sum_g / count as f32,
            sum_b / count as f32,
        );
        self.global_color_cache.insert(key, avg);
        Some(avg)
    }

    fn apply_seam_blend(&mut self, namespace: &str, coord: TileCoord, out: &mut RgbImage) {
        let feather = self.config.seam_feather_px.clamp(1, TILE_SIZE / 2);
        let neighbor_weight = self.config.seam_neighbor_weight.clamp(0.0, 4.0);
        if neighbor_weight <= 0.0 {
            return;
        }

        let left = if coord.x > 0 {
            self.load_good_tile(namespace, coord.z, coord.x - 1, coord.y)
                .map(|(_, img, _)| img)
        } else {
            None
        };
        let right = coord.x.checked_add(1).and_then(|nx| {
            self.load_good_tile(namespace, coord.z, nx, coord.y)
                .map(|(_, img, _)| img)
        });
        let top = if coord.y > 0 {
            self.load_good_tile(namespace, coord.z, coord.x, coord.y - 1)
                .map(|(_, img, _)| img)
        } else {
            None
        };
        let bottom = coord.y.checked_add(1).and_then(|ny| {
            self.load_good_tile(namespace, coord.z, coord.x, ny)
                .map(|(_, img, _)| img)
        });

        if left.is_none() && right.is_none() && top.is_none() && bottom.is_none() {
            return;
        }

        for y in 0..TILE_SIZE {
            for x in 0..TILE_SIZE {
                let base = out.get_pixel(x, y).channels();
                let mut rr = base[0] as f32;
                let mut gg = base[1] as f32;
                let mut bb = base[2] as f32;
                let mut total_w = 1.0f32;

                if let Some(ref img) = left {
                    if x < feather {
                        let w = ((feather - x) as f32 / feather as f32) * neighbor_weight;
                        let nx = TILE_SIZE - feather + x;
                        let p = img.get_pixel(nx, y).channels();
                        rr += p[0] as f32 * w;
                        gg += p[1] as f32 * w;
                        bb += p[2] as f32 * w;
                        total_w += w;
                    }
                }
                if let Some(ref img) = right {
                    if x >= TILE_SIZE - feather {
                        let d = x - (TILE_SIZE - feather);
                        let w = ((d + 1) as f32 / feather as f32) * neighbor_weight;
                        let p = img.get_pixel(d, y).channels();
                        rr += p[0] as f32 * w;
                        gg += p[1] as f32 * w;
                        bb += p[2] as f32 * w;
                        total_w += w;
                    }
                }
                if let Some(ref img) = top {
                    if y < feather {
                        let w = ((feather - y) as f32 / feather as f32) * neighbor_weight;
                        let ny = TILE_SIZE - feather + y;
                        let p = img.get_pixel(x, ny).channels();
                        rr += p[0] as f32 * w;
                        gg += p[1] as f32 * w;
                        bb += p[2] as f32 * w;
                        total_w += w;
                    }
                }
                if let Some(ref img) = bottom {
                    if y >= TILE_SIZE - feather {
                        let d = y - (TILE_SIZE - feather);
                        let w = ((d + 1) as f32 / feather as f32) * neighbor_weight;
                        let p = img.get_pixel(x, d).channels();
                        rr += p[0] as f32 * w;
                        gg += p[1] as f32 * w;
                        bb += p[2] as f32 * w;
                        total_w += w;
                    }
                }

                out.put_pixel(
                    x,
                    y,
                    Rgb([
                        clamp_u8(rr / total_w),
                        clamp_u8(gg / total_w),
                        clamp_u8(bb / total_w),
                    ]),
                );
            }
        }
    }
}

fn truncate_sources(mut srcs: Vec<SourceTileRef>) -> Vec<SourceTileRef> {
    if srcs.len() > 64 {
        srcs.truncate(64);
    }
    srcs
}

fn paste(dst: &mut RgbImage, src: &RgbImage, offset_x: u32, offset_y: u32) {
    for y in 0..src.height() {
        for x in 0..src.width() {
            let p = src.get_pixel(x, y);
            dst.put_pixel(offset_x + x, offset_y + y, *p);
        }
    }
}

fn blend_images(a: &RgbImage, b: &RgbImage, alpha: f32) -> RgbImage {
    let mut out = RgbImage::new(TILE_SIZE, TILE_SIZE);
    let a_alpha = (1.0 - alpha).clamp(0.0, 1.0);
    let b_alpha = alpha.clamp(0.0, 1.0);
    for y in 0..TILE_SIZE {
        for x in 0..TILE_SIZE {
            let pa = a.get_pixel(x, y).channels();
            let pb = b.get_pixel(x, y).channels();
            let rr = pa[0] as f32 * a_alpha + pb[0] as f32 * b_alpha;
            let gg = pa[1] as f32 * a_alpha + pb[1] as f32 * b_alpha;
            let bb = pa[2] as f32 * a_alpha + pb[2] as f32 * b_alpha;
            out.put_pixel(x, y, Rgb([clamp_u8(rr), clamp_u8(gg), clamp_u8(bb)]));
        }
    }
    out
}

fn compute_image_metrics(img: &RgbImage) -> ImageMetrics {
    let mut sum_l = 0.0f64;
    let mut sum_sq_l = 0.0f64;
    let mut min_l = 255.0f64;
    let mut max_l = 0.0f64;
    let mut sum_r = 0.0f64;
    let mut sum_g = 0.0f64;
    let mut sum_b = 0.0f64;

    let n = (img.width() as f64) * (img.height() as f64);
    for p in img.pixels() {
        let [r, g, b] = p.0;
        let rf = r as f64;
        let gf = g as f64;
        let bf = b as f64;
        let l = 0.2126 * rf + 0.7152 * gf + 0.0722 * bf;
        sum_l += l;
        sum_sq_l += l * l;
        min_l = min_l.min(l);
        max_l = max_l.max(l);
        sum_r += rf;
        sum_g += gf;
        sum_b += bf;
    }

    let mean_l = if n > 0.0 { sum_l / n } else { 0.0 };
    let var = if n > 0.0 {
        (sum_sq_l / n) - (mean_l * mean_l)
    } else {
        0.0
    }
    .max(0.0);

    ImageMetrics {
        mean_luma: mean_l as f32,
        std_luma: var.sqrt() as f32,
        mean_r: if n > 0.0 { (sum_r / n) as f32 } else { 0.0 },
        mean_g: if n > 0.0 { (sum_g / n) as f32 } else { 0.0 },
        mean_b: if n > 0.0 { (sum_b / n) as f32 } else { 0.0 },
        dynamic_range: (max_l - min_l) as f32,
    }
}

fn quality_score(metrics: ImageMetrics) -> f32 {
    (metrics.std_luma * 4.0)
        + (metrics.dynamic_range * 0.20)
        + ((metrics.mean_luma - 64.0).abs() * -0.03)
        + 12.0
}

fn apply_context_tuning(img: &mut RgbImage, context: ContextKind, cfg: &SuperFillConfig) {
    for px in img.pixels_mut() {
        let mut r = px[0] as f32;
        let mut g = px[1] as f32;
        let mut b = px[2] as f32;

        let luma = 0.299 * r + 0.587 * g + 0.114 * b;
        r = luma + (r - luma) * cfg.global_saturation;
        g = luma + (g - luma) * cfg.global_saturation;
        b = luma + (b - luma) * cfg.global_saturation;

        r = 128.0 + (r - 128.0) * cfg.global_contrast;
        g = 128.0 + (g - 128.0) * cfg.global_contrast;
        b = 128.0 + (b - 128.0) * cfg.global_contrast;

        match context {
            ContextKind::Water => {
                b *= cfg.water_blue_gain;
                g *= cfg.water_green_gain;
            }
            ContextKind::Greenery => {
                g *= cfg.greenery_green_gain;
                b *= cfg.greenery_blue_gain;
            }
            ContextKind::Neutral => {}
        }

        *px = Rgb([clamp_u8(r), clamp_u8(g), clamp_u8(b)]);
    }
}

fn clamp_u8(v: f32) -> u8 {
    v.round().clamp(0.0, 255.0) as u8
}

pub fn write_worldfiles_png(path: &Path, zoom: u32, x: u32, y: u32) -> Result<()> {
    let n = 1u64 << zoom;
    let res = WEBM_WORLD / ((n as f64) * (TILE_SIZE as f64));
    let minx = -WEBM_HALF + (x as f64) * (TILE_SIZE as f64) * res;
    let maxy = WEBM_HALF - (y as f64) * (TILE_SIZE as f64) * res;
    let c = minx + res / 2.0;
    let f = maxy - res / 2.0;
    let txt = format!(
        "{res:.12}\n0.000000000000\n0.000000000000\n{neg:.12}\n{c:.12}\n{f:.12}\n",
        neg = -res
    );

    fs::write(path.with_extension("pgw"), &txt)?;
    fs::write(PathBuf::from(format!("{}.pgw", path.display())), &txt)?;
    Ok(())
}
