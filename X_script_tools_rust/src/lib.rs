use std::collections::HashSet;
use std::f32::consts::PI;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use image::imageops::{self, FilterType};
use image::{DynamicImage, GrayImage, ImageBuffer, Rgb, RgbImage};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

const HIST_BINS: usize = 32;
const HASH_SIZE: u32 = 16;
const DEFAULT_PATCH_LIBRARY: &str = "crates/10_step_generative_fix/patch_library";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PatchCatalogEntry {
    pub file: String,
    pub path: String,
    pub size_bytes: u64,
    pub mean_rgb: [f32; 3],
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LibraryMatch {
    pub path: String,
    pub hist_dist: f32,
    pub phash_dist: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileScanRecord {
    pub path: String,
    pub size: Option<u64>,
    pub mean_rgb: Option<[f32; 3]>,
    pub std_rgb: Option<[f32; 3]>,
    pub gray_std: Option<f32>,
    pub context_class: Option<String>,
    pub md5: Option<String>,
    pub is_bad: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TileScanOptions {
    pub tiny_bytes: u64,
    pub flat_std_threshold: f32,
    pub dark_mean_threshold: f32,
    pub dark_std_threshold: f32,
    pub placeholder_hashes: HashSet<String>,
}

impl Default for TileScanOptions {
    fn default() -> Self {
        Self {
            tiny_bytes: 200,
            flat_std_threshold: 3.0,
            dark_mean_threshold: 30.0,
            dark_std_threshold: 10.0,
            placeholder_hashes: HashSet::new(),
        }
    }
}

pub fn default_patch_library_path() -> PathBuf {
    PathBuf::from(DEFAULT_PATCH_LIBRARY)
}

pub fn default_patch_catalog_output(library_dir: &Path) -> PathBuf {
    library_dir.join("catalog.json")
}

pub fn read_placeholder_hashes(path: &Path) -> Result<HashSet<String>> {
    let raw = fs::read_to_string(path)
        .with_context(|| format!("Failed to read placeholder hash file {}", path.display()))?;
    Ok(raw
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(|line| line.to_ascii_lowercase())
        .collect())
}

pub fn generate_patch_catalog(
    library_dir: &Path,
    out_path: &Path,
) -> Result<Vec<PatchCatalogEntry>> {
    let mut entries = Vec::new();
    let mut files = fs::read_dir(library_dir)
        .with_context(|| format!("Failed to read library dir {}", library_dir.display()))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .collect::<Vec<_>>();
    files.sort();

    for path in files {
        if !is_patch_source_image(&path) {
            continue;
        }
        let image =
            image::open(&path).with_context(|| format!("Failed to open {}", path.display()))?;
        let mean_rgb = compute_mean_rgb(&image.to_rgb8());
        let file_name = path
            .file_name()
            .and_then(|value| value.to_str())
            .unwrap_or("patch.png")
            .to_string();
        entries.push(PatchCatalogEntry {
            file: file_name,
            path: normalize_display_path(&path),
            size_bytes: fs::metadata(&path)?.len(),
            mean_rgb: round_rgb(mean_rgb),
            tags: Vec::new(),
        });
    }

    if let Some(parent) = out_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(out_path, serde_json::to_string_pretty(&entries)?)?;
    Ok(entries)
}

pub fn match_library(
    target_path: &Path,
    library_dir: &Path,
    topk: usize,
) -> Result<Vec<LibraryMatch>> {
    let image = image::open(target_path)
        .with_context(|| format!("Failed to open target image {}", target_path.display()))?;
    match_library_image(&image, library_dir, topk)
}

pub fn match_library_image(
    target_image: &DynamicImage,
    library_dir: &Path,
    topk: usize,
) -> Result<Vec<LibraryMatch>> {
    let target_hist = normalized_histogram(target_image);
    let target_hash = average_hash(target_image);

    let mut matches = Vec::new();
    let mut files = fs::read_dir(library_dir)
        .with_context(|| format!("Failed to read library dir {}", library_dir.display()))?
        .filter_map(|entry| entry.ok().map(|value| value.path()))
        .collect::<Vec<_>>();
    files.sort();

    for path in files {
        if !is_match_candidate(&path) {
            continue;
        }
        let image = match image::open(&path) {
            Ok(value) => value,
            Err(_) => continue,
        };
        let hist = normalized_histogram(&image);
        let hash = average_hash(&image);
        matches.push(LibraryMatch {
            path: normalize_display_path(&path),
            hist_dist: l2_distance(&target_hist, &hist),
            phash_dist: hamming_distance(&target_hash, &hash),
        });
    }

    matches.sort_by(|left, right| {
        left.hist_dist
            .partial_cmp(&right.hist_dist)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then(left.phash_dist.cmp(&right.phash_dist))
            .then(left.path.cmp(&right.path))
    });
    matches.truncate(topk.max(1));
    Ok(matches)
}

pub fn generate_patterns(out_dir: &Path, size: u32, presets: &[String]) -> Result<Vec<PathBuf>> {
    fs::create_dir_all(out_dir)?;
    let mut generated = Vec::new();
    for preset in presets {
        let image = build_preset_image(preset, size);
        let output = out_dir.join(format!("{preset}_{size}.png"));
        image.save(&output)?;
        let thumb = DynamicImage::ImageRgb8(image.clone());
        let blurred = imageops::blur(&thumb, 2.0);
        let thumb_path = out_dir.join(format!("{preset}_{size}_thumb.png"));
        blurred.save(&thumb_path)?;
        generated.push(output);
    }
    Ok(generated)
}

pub fn scan_tiles(root: &Path, options: &TileScanOptions) -> Result<Vec<TileScanRecord>> {
    let mut results = Vec::new();
    for entry in WalkDir::new(root).follow_links(false) {
        let entry = match entry {
            Ok(value) => value,
            Err(_) => continue,
        };
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path().to_path_buf();
        if !is_match_candidate(&path) {
            continue;
        }

        match scan_one_tile(&path, options) {
            Ok(record) => results.push(record),
            Err(error) => results.push(TileScanRecord {
                path: normalize_display_path(&path),
                size: None,
                mean_rgb: None,
                std_rgb: None,
                gray_std: None,
                context_class: None,
                md5: None,
                is_bad: true,
                reason: None,
                error: Some(error.to_string()),
            }),
        }
    }
    Ok(results)
}

fn scan_one_tile(path: &Path, options: &TileScanOptions) -> Result<TileScanRecord> {
    let data = fs::read(path)?;
    let md5 = format!("{:x}", md5::compute(&data));
    let image =
        image::open(path).with_context(|| format!("Failed to open tile {}", path.display()))?;
    let rgb = image.to_rgb8();
    let size = data.len() as u64;
    let mean_rgb = compute_mean_rgb(&rgb);
    let std_rgb = compute_channel_std(&rgb, mean_rgb);
    let gray_std = compute_gray_std(&rgb);
    let context = context_class_from_mean(mean_rgb);

    let mut is_bad = false;
    let mut reason = None;
    if size <= options.tiny_bytes {
        is_bad = true;
        reason = Some("tiny_bytes".to_string());
    } else if gray_std <= options.flat_std_threshold {
        is_bad = true;
        reason = Some("flat_std_low".to_string());
    } else if mean_rgb.iter().sum::<f32>() <= options.dark_mean_threshold * 3.0
        && gray_std <= options.dark_std_threshold
    {
        is_bad = true;
        reason = Some("dark_flat".to_string());
    } else if options
        .placeholder_hashes
        .contains(&md5.to_ascii_lowercase())
    {
        is_bad = true;
        reason = Some("placeholder_hash".to_string());
    }

    Ok(TileScanRecord {
        path: normalize_display_path(path),
        size: Some(size),
        mean_rgb: Some(round_rgb(mean_rgb)),
        std_rgb: Some(round_rgb(std_rgb)),
        gray_std: Some(round_value(gray_std)),
        context_class: Some(context.to_string()),
        md5: Some(md5),
        is_bad,
        reason,
        error: None,
    })
}

fn is_patch_source_image(path: &Path) -> bool {
    let Some(file_name) = path.file_name().and_then(|value| value.to_str()) else {
        return false;
    };
    path.extension()
        .and_then(|value| value.to_str())
        .map(|ext| ext.eq_ignore_ascii_case("png"))
        .unwrap_or(false)
        && !file_name.to_ascii_lowercase().ends_with("_thumb.png")
}

fn is_match_candidate(path: &Path) -> bool {
    path.extension()
        .and_then(|value| value.to_str())
        .map(|ext| {
            ext.eq_ignore_ascii_case("png")
                || ext.eq_ignore_ascii_case("jpg")
                || ext.eq_ignore_ascii_case("jpeg")
                || ext.eq_ignore_ascii_case("webp")
        })
        .unwrap_or(false)
}

fn normalize_display_path(path: &Path) -> String {
    path.display().to_string().replace('\\', "/")
}

fn compute_mean_rgb(image: &RgbImage) -> [f32; 3] {
    let mut totals = [0f64; 3];
    let pixel_count = (image.width() * image.height()).max(1) as f64;
    for pixel in image.pixels() {
        totals[0] += pixel[0] as f64;
        totals[1] += pixel[1] as f64;
        totals[2] += pixel[2] as f64;
    }
    [
        (totals[0] / pixel_count) as f32,
        (totals[1] / pixel_count) as f32,
        (totals[2] / pixel_count) as f32,
    ]
}

fn compute_channel_std(image: &RgbImage, mean: [f32; 3]) -> [f32; 3] {
    let mut totals = [0f64; 3];
    let pixel_count = (image.width() * image.height()).max(1) as f64;
    for pixel in image.pixels() {
        totals[0] += ((pixel[0] as f32 - mean[0]).powi(2)) as f64;
        totals[1] += ((pixel[1] as f32 - mean[1]).powi(2)) as f64;
        totals[2] += ((pixel[2] as f32 - mean[2]).powi(2)) as f64;
    }
    [
        (totals[0] / pixel_count).sqrt() as f32,
        (totals[1] / pixel_count).sqrt() as f32,
        (totals[2] / pixel_count).sqrt() as f32,
    ]
}

fn compute_gray_std(image: &RgbImage) -> f32 {
    let pixel_count = (image.width() * image.height()).max(1) as f64;
    let mut total = 0f64;
    let mut total_sq = 0f64;
    for pixel in image.pixels() {
        let gray = 0.2989 * pixel[0] as f64 + 0.5870 * pixel[1] as f64 + 0.1140 * pixel[2] as f64;
        total += gray;
        total_sq += gray * gray;
    }
    let mean = total / pixel_count;
    (total_sq / pixel_count - mean * mean).max(0.0).sqrt() as f32
}

fn normalized_histogram(image: &DynamicImage) -> Vec<f32> {
    let rgb = image.to_rgb8();
    let mut hist = vec![0f32; HIST_BINS * 3];
    let pixel_count = (rgb.width() * rgb.height()).max(1) as f32;
    for pixel in rgb.pixels() {
        for channel in 0..3 {
            let value = pixel[channel] as usize;
            let bin = (value * HIST_BINS) / 256;
            hist[channel * HIST_BINS + bin.min(HIST_BINS - 1)] += 1.0;
        }
    }
    for value in &mut hist {
        *value /= pixel_count;
    }
    hist
}

fn average_hash(image: &DynamicImage) -> Vec<u8> {
    let gray: GrayImage = image
        .resize_exact(HASH_SIZE, HASH_SIZE, FilterType::Lanczos3)
        .grayscale()
        .to_luma8();
    let pixel_count = (HASH_SIZE * HASH_SIZE) as f32;
    let mean = gray.pixels().map(|pixel| pixel[0] as f32).sum::<f32>() / pixel_count;
    gray.pixels()
        .map(|pixel| u8::from(pixel[0] as f32 > mean))
        .collect()
}

fn hamming_distance(left: &[u8], right: &[u8]) -> u32 {
    left.iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| u32::from(lhs != rhs))
        .sum()
}

fn l2_distance(left: &[f32], right: &[f32]) -> f32 {
    left.iter()
        .zip(right.iter())
        .map(|(lhs, rhs)| {
            let diff = lhs - rhs;
            diff * diff
        })
        .sum::<f32>()
        .sqrt()
}

fn round_value(value: f32) -> f32 {
    (value * 100.0).round() / 100.0
}

fn round_rgb(values: [f32; 3]) -> [f32; 3] {
    [
        round_value(values[0]),
        round_value(values[1]),
        round_value(values[2]),
    ]
}

fn context_class_from_mean(mean_rgb: [f32; 3]) -> &'static str {
    let [r, g, b] = mean_rgb;
    if b > g * 1.05 && b > r * 1.05 {
        "water"
    } else if g > b * 1.05 && g > r * 1.05 {
        "greenery"
    } else {
        "neutral"
    }
}

fn build_preset_image(preset: &str, size: u32) -> RgbImage {
    let mut out = ImageBuffer::new(size, size);
    for y in 0..size {
        for x in 0..size {
            let pixel = match preset {
                "water" => water_pixel(x, y, size, [60.0, 110.0, 180.0], 11),
                "dark_water" => water_pixel(x, y, size, [20.0, 60.0, 110.0], 29),
                "light_water" => water_pixel(x, y, size, [120.0, 170.0, 210.0], 47),
                "grass" => grass_pixel(x, y, size, [80.0, 140.0, 70.0], 67),
                "dark_grass" => grass_pixel(x, y, size, [40.0, 90.0, 40.0], 83),
                "light_grass" => grass_pixel(x, y, size, [120.0, 180.0, 100.0], 101),
                "brown_mountain" => mountain_pixel(x, y, size, [110.0, 80.0, 60.0], 131),
                _ => generic_pixel(x, y, size, 157),
            };
            out.put_pixel(x, y, Rgb(pixel));
        }
    }
    out
}

fn water_pixel(x: u32, y: u32, size: u32, base: [f32; 3], seed: u32) -> [u8; 3] {
    let noise = fbm_noise(x, y, size, seed);
    let ripple = ((x as f32 / size.max(1) as f32) * PI * 4.0).sin() * 10.0;
    [
        clamp_u8(base[0] + noise * 12.0),
        clamp_u8(base[1] + noise * 12.0),
        clamp_u8(base[2] + noise * 20.0 + ripple),
    ]
}

fn grass_pixel(x: u32, y: u32, size: u32, base: [f32; 3], seed: u32) -> [u8; 3] {
    let noise = fbm_noise(x, y, size, seed);
    let grain = hash_noise(x / 2, y * 3, seed + 17) * 8.0;
    [
        clamp_u8(base[0] + noise * 8.0),
        clamp_u8(base[1] + noise * 20.0 + grain),
        clamp_u8(base[2] + noise * 8.0),
    ]
}

fn mountain_pixel(x: u32, y: u32, size: u32, base: [f32; 3], seed: u32) -> [u8; 3] {
    let noise = fbm_noise(x, y, size, seed);
    let ridge = ((x as f32 * 0.11 + y as f32 * 0.18).sin() + 1.0) * 0.5;
    [
        clamp_u8(base[0] + noise * 24.0 - ridge * 12.0),
        clamp_u8(base[1] + noise * 16.0 - ridge * 8.0),
        clamp_u8(base[2] + noise * 10.0 - ridge * 6.0),
    ]
}

fn generic_pixel(x: u32, y: u32, size: u32, seed: u32) -> [u8; 3] {
    let noise = fbm_noise(x, y, size, seed);
    let value = 128.0 + noise * 22.0;
    [clamp_u8(value), clamp_u8(value), clamp_u8(value)]
}

fn fbm_noise(x: u32, y: u32, size: u32, seed: u32) -> f32 {
    let scale = size.max(1) as f32;
    let x = x as f32 / scale;
    let y = y as f32 / scale;
    let mut total = 0.0;
    total += hash_noise_f32(x * 8.0, y * 8.0, seed) * 0.6;
    total += hash_noise_f32(x * 16.0, y * 16.0, seed.wrapping_add(13)) * 0.3;
    total += hash_noise_f32(x * 32.0, y * 32.0, seed.wrapping_add(29)) * 0.1;
    total
}

fn hash_noise_f32(x: f32, y: f32, seed: u32) -> f32 {
    let xi = (x * 997.0) as u32;
    let yi = (y * 991.0) as u32;
    hash_noise(xi, yi, seed)
}

fn hash_noise(x: u32, y: u32, seed: u32) -> f32 {
    let mut value = x.wrapping_mul(0x45d9f3b) ^ y.wrapping_mul(0x119de1f3) ^ seed;
    value ^= value >> 16;
    value = value.wrapping_mul(0x45d9f3b);
    value ^= value >> 16;
    let normalized = (value as f64 / u32::MAX as f64) * 2.0 - 1.0;
    normalized as f32
}

fn clamp_u8(value: f32) -> u8 {
    value.round().clamp(0.0, 255.0) as u8
}
