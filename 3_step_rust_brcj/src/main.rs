use clap::{ArgGroup, Parser};
use image::{DynamicImage, ImageBuffer, ImageReader, Rgba, RgbaImage};
use imageproc::filter::gaussian_blur_f32;
use indicatif::{ProgressBar, ProgressStyle};
use pipeline_core::{ProgressReporter, StepTimer};
use rayon::prelude::*;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use walkdir::WalkDir;

/// BRCJ "BABE RUTH CRAZY JEFF" darkener in Rust
///
/// Modes:
///   - High:  Z17 / Z18 / Z19
///   - Mid:   Z15 / Z16
///   - Z14:   Z14
///   - Z13:   Z13 (Heaven)
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
#[command(group(
    ArgGroup::new("mode_group")
        .required(false)
        .args(&["group", "zoom"]),
))]
struct Cli {
    /// Input directory containing tiles
    #[arg(short = 'i', long)]
    input: PathBuf,

    /// Output directory for processed tiles
    #[arg(short = 'o', long)]
    output: PathBuf,

    /// Explicit group: high, mid, z14, z13
    #[arg(long)]
    group: Option<String>,

    /// Zoom level (13–19) to auto-pick group
    #[arg(long)]
    zoom: Option<u8>,

    /// Number of worker threads (default: Rayon automatic)
    #[arg(long)]
    workers: Option<usize>,

    /// Overwrite existing output files
    #[arg(long)]
    overwrite: bool,
}

#[derive(Clone, Copy, Debug)]
struct BrParameters {
    // Curve
    curve_in: [f32; 3],
    curve_out: [f32; 3],

    // Blur
    gauss_blur_radius: f32,
    smart_blur_radius: Option<f32>,
    smart_blur_threshold: Option<f32>,

    // Exposure / gamma
    exposure_offset: f32,
    gamma: f32,

    // Unsharp
    unsharp_radius: f32,
    unsharp_percent: f32,
    unsharp_threshold: f32,

    // Noise
    reduce_noise_strength: f32,

    // Brightness / Contrast
    brightness_factor: f32,
    contrast_factor: f32,

    // HSL
    hue_shift_deg: f32,
    sat_factor: f32,
    lightness_add: f32,

    // Vibrance
    vibrance_strength: f32,
    vibrance_sat_factor: f32,
}

// ---------------- PARAM SETS ----------------

// HIGH RES: Z17 / Z18 / Z19
fn params_high() -> BrParameters {
    BrParameters {
        curve_in: [0.0, 20.0 / 255.0, 1.0],
        curve_out: [0.0, 0.0 / 255.0, 180.0 / 255.0],
        gauss_blur_radius: 0.3,
        smart_blur_radius: None,
        smart_blur_threshold: None,
        exposure_offset: -0.003,
        gamma: 1.00,
        unsharp_radius: 1.0,
        unsharp_percent: 0.99,
        unsharp_threshold: 0.0,
        reduce_noise_strength: 0.99,
        brightness_factor: 0.95,
        contrast_factor: 1.10,
        hue_shift_deg: 0.0,
        sat_factor: 1.10,
        lightness_add: 0.0,
        vibrance_strength: 1.10,
        vibrance_sat_factor: 0.0,
    }
}

// MID RES: Z15 / Z16
fn params_mid() -> BrParameters {
    BrParameters {
        curve_in: [0.0, 20.0 / 255.0, 1.0],
        curve_out: [0.0, 0.0 / 255.0, 180.0 / 255.0],
        gauss_blur_radius: 0.3,
        smart_blur_radius: None,
        smart_blur_threshold: None,
        exposure_offset: -0.003,
        gamma: 1.00,
        // Mid-res override: sharpen less to avoid chunky noise
        unsharp_radius: 0.2,
        unsharp_percent: 0.99,
        unsharp_threshold: 0.0,
        reduce_noise_strength: 0.99,
        brightness_factor: 0.95,
        contrast_factor: 1.10,
        hue_shift_deg: 0.0,
        sat_factor: 1.10,
        lightness_add: 0.0,
        vibrance_strength: 1.10,
        vibrance_sat_factor: 0.0,
    }
}

// LOW RES: Z14
fn params_z14() -> BrParameters {
    BrParameters {
        curve_in: [0.0, 20.0 / 255.0, 1.0],
        curve_out: [0.0, 0.0 / 255.0, 180.0 / 255.0],
        gauss_blur_radius: 0.3,
        smart_blur_radius: Some(0.5), // Smart blur
        smart_blur_threshold: Some(70.0),
        exposure_offset: -0.003,
        gamma: 1.00,
        unsharp_radius: 0.2,
        unsharp_percent: 0.99,
        unsharp_threshold: 0.0,
        reduce_noise_strength: 0.99,
        brightness_factor: 0.95,
        contrast_factor: 1.10,
        hue_shift_deg: 0.0,
        sat_factor: 1.10,
        lightness_add: 0.0,
        vibrance_strength: 1.10,
        vibrance_sat_factor: 0.0,
    }
}

// LOW RES HEAVEN: Z13
fn params_z13() -> BrParameters {
    BrParameters {
        curve_in: [0.0, 20.0 / 255.0, 1.0],
        curve_out: [0.0, 0.0 / 255.0, 180.0 / 255.0],
        gauss_blur_radius: 0.3,
        smart_blur_radius: Some(0.5),
        smart_blur_threshold: Some(99.0),
        exposure_offset: -0.003,
        gamma: 1.00,
        unsharp_radius: 0.2,
        unsharp_percent: 0.99,
        unsharp_threshold: 0.0,
        reduce_noise_strength: 0.99,
        brightness_factor: 0.95,
        contrast_factor: 1.10,
        hue_shift_deg: 0.0,
        sat_factor: 1.10,
        lightness_add: 0.0,
        vibrance_strength: 1.10,
        vibrance_sat_factor: 0.0,
    }
}

// -------------- PIPELINE HELPERS --------------

fn clamp01(x: f32) -> f32 {
    x.max(0.0).min(1.0)
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
    a + (b - a) * t
}

fn apply_curve_luma(img: &mut RgbaImage, params: &BrParameters) {
    let [x0, x1, x2] = params.curve_in;
    let [y0, y1, y2] = params.curve_out;

    for pixel in img.pixels_mut() {
        let r = pixel[0] as f32 / 255.0;
        let g = pixel[1] as f32 / 255.0;
        let b = pixel[2] as f32 / 255.0;

        // Simple luma
        let l = 0.299 * r + 0.587 * g + 0.114 * b;

        let y = if l <= x1 {
            let t = if x1 - x0 == 0.0 {
                0.0
            } else {
                (l - x0) / (x1 - x0)
            };
            lerp(y0, y1, t)
        } else {
            let t = if x2 - x1 == 0.0 {
                0.0
            } else {
                (l - x1) / (x2 - x1)
            };
            lerp(y1, y2, t)
        };

        let scale = if l > 0.0 { y / l } else { 1.0 };
        let nr = clamp01(r * scale);
        let ng = clamp01(g * scale);
        let nb = clamp01(b * scale);

        pixel[0] = (nr * 255.0).round() as u8;
        pixel[1] = (ng * 255.0).round() as u8;
        pixel[2] = (nb * 255.0).round() as u8;
    }
}

fn apply_gaussian_blur(img: &RgbaImage, radius: f32) -> RgbaImage {
    if radius <= 0.0 {
        return img.clone();
    }

    let (w, h) = img.dimensions();
    let mut r = ImageBuffer::new(w, h);
    let mut g = ImageBuffer::new(w, h);
    let mut b = ImageBuffer::new(w, h);
    let mut a = ImageBuffer::new(w, h);

    for (x, y, p) in img.enumerate_pixels() {
        r.put_pixel(x, y, image::Luma([p[0]]));
        g.put_pixel(x, y, image::Luma([p[1]]));
        b.put_pixel(x, y, image::Luma([p[2]]));
        a.put_pixel(x, y, image::Luma([p[3]]));
    }

    let r_blur = gaussian_blur_f32(&r, radius);
    let g_blur = gaussian_blur_f32(&g, radius);
    let b_blur = gaussian_blur_f32(&b, radius);
    let a_blur = gaussian_blur_f32(&a, radius);

    let mut out = ImageBuffer::new(w, h);
    for y in 0..h {
        for x in 0..w {
            let rr = r_blur.get_pixel(x, y)[0];
            let gg = g_blur.get_pixel(x, y)[0];
            let bb = b_blur.get_pixel(x, y)[0];
            let aa = a_blur.get_pixel(x, y)[0];
            out.put_pixel(x, y, Rgba([rr, gg, bb, aa]));
        }
    }

    out
}

fn apply_exposure_and_gamma(img: &mut RgbaImage, params: &BrParameters) {
    let offset = params.exposure_offset;
    let gamma = if params.gamma == 0.0 {
        1.0
    } else {
        params.gamma
    };

    for p in img.pixels_mut() {
        for c in 0..3 {
            let mut v = p[c] as f32 / 255.0;
            v += offset;
            v = clamp01(v);
            v = v.powf(1.0 / gamma);
            p[c] = (v * 255.0).round() as u8;
        }
    }
}

fn apply_unsharp_mask(base: &RgbaImage, radius: f32, amount: f32, _threshold: f32) -> RgbaImage {
    if radius <= 0.0 || amount <= 0.0 {
        return base.clone();
    }

    let blurred = apply_gaussian_blur(base, radius);
    let (w, h) = base.dimensions();
    let mut out = ImageBuffer::new(w, h);

    for y in 0..h {
        for x in 0..w {
            let orig = base.get_pixel(x, y);
            let blur = blurred.get_pixel(x, y);

            let mut res = [0u8; 4];
            for c in 0..3 {
                let o = orig[c] as f32;
                let b = blur[c] as f32;
                let diff = o - b;
                let v = (o + amount * diff).round().clamp(0.0, 255.0);
                res[c] = v as u8;
            }
            res[3] = orig[3];
            out.put_pixel(x, y, Rgba(res));
        }
    }

    out
}

fn apply_noise_reduction(img: &mut RgbaImage, strength: f32) {
    if strength <= 0.0 {
        return;
    }
    let (w, h) = img.dimensions();
    let tmp = img.clone();
    let blurred = apply_gaussian_blur(&tmp, 0.6 * strength);

    for y in 0..h {
        for x in 0..w {
            let orig = img.get_pixel(x, y);
            let b = blurred.get_pixel(x, y);
            let mut out = *orig;
            for c in 0..3 {
                let o = orig[c] as f32;
                let bb = b[c] as f32;
                let v = o * (1.0 - strength * 0.5) + bb * (strength * 0.5);
                out[c] = v.round().clamp(0.0, 255.0) as u8;
            }
            img.put_pixel(x, y, out);
        }
    }
}

fn apply_brightness_contrast(img: &mut RgbaImage, brightness: f32, contrast: f32) {
    for p in img.pixels_mut() {
        for c in 0..3 {
            let mut v = p[c] as f32 / 255.0;
            v *= brightness;
            v = (v - 0.5) * contrast + 0.5;
            v = clamp01(v);
            p[c] = (v * 255.0).round() as u8;
        }
    }
}

// Basic RGB <-> HSL utilities
fn rgb_to_hsl(r: f32, g: f32, b: f32) -> (f32, f32, f32) {
    let max = r.max(g.max(b));
    let min = r.min(g.min(b));
    let l = (max + min) / 2.0;
    if max == min {
        return (0.0, 0.0, l);
    }
    let d = max - min;
    let s = if l > 0.5 {
        d / (2.0 - max - min)
    } else {
        d / (max + min)
    };
    let h = if max == r {
        ((g - b) / d + if g < b { 6.0 } else { 0.0 }) / 6.0
    } else if max == g {
        ((b - r) / d + 2.0) / 6.0
    } else {
        ((r - g) / d + 4.0) / 6.0
    };
    (h, s, l)
}

fn hue_to_rgb(p: f32, q: f32, t: f32) -> f32 {
    let mut t = t;
    if t < 0.0 {
        t += 1.0;
    }
    if t > 1.0 {
        t -= 1.0;
    }
    if t < 1.0 / 6.0 {
        return p + (q - p) * 6.0 * t;
    }
    if t < 1.0 / 2.0 {
        return q;
    }
    if t < 2.0 / 3.0 {
        return p + (q - p) * (2.0 / 3.0 - t) * 6.0;
    }
    p
}

fn hsl_to_rgb(h: f32, s: f32, l: f32) -> (f32, f32, f32) {
    if s == 0.0 {
        return (l, l, l);
    }
    let q = if l < 0.5 {
        l * (1.0 + s)
    } else {
        l + s - l * s
    };
    let p = 2.0 * l - q;
    let r = hue_to_rgb(p, q, h + 1.0 / 3.0);
    let g = hue_to_rgb(p, q, h);
    let b = hue_to_rgb(p, q, h - 1.0 / 3.0);
    (r, g, b)
}

fn apply_hsl_adjustments(img: &mut RgbaImage, params: &BrParameters) {
    let hue_shift = params.hue_shift_deg / 360.0;
    let sat_factor = params.sat_factor;
    let light_add = params.lightness_add;

    for p in img.pixels_mut() {
        let r = p[0] as f32 / 255.0;
        let g = p[1] as f32 / 255.0;
        let b = p[2] as f32 / 255.0;
        let (mut h, mut s, mut l) = rgb_to_hsl(r, g, b);

        h = (h + hue_shift).rem_euclid(1.0);
        s = clamp01(s * sat_factor);
        l = clamp01(l + light_add);

        let (nr, ng, nb) = hsl_to_rgb(h, s, l);
        p[0] = (nr * 255.0).round() as u8;
        p[1] = (ng * 255.0).round() as u8;
        p[2] = (nb * 255.0).round() as u8;
    }
}

fn apply_vibrance(img: &mut RgbaImage, params: &BrParameters) {
    let strength = params.vibrance_strength;
    let extra_sat = params.vibrance_sat_factor;

    if strength <= 0.0 && extra_sat <= 0.0 {
        return;
    }

    for p in img.pixels_mut() {
        let r = p[0] as f32 / 255.0;
        let g = p[1] as f32 / 255.0;
        let b = p[2] as f32 / 255.0;
        let (h, mut s, l) = rgb_to_hsl(r, g, b);

        let weight = 1.0 - s;
        s *= 1.0 + strength * weight;
        s += extra_sat;
        s = clamp01(s);

        let (nr, ng, nb) = hsl_to_rgb(h, s, l);
        p[0] = (nr * 255.0).round() as u8;
        p[1] = (ng * 255.0).round() as u8;
        p[2] = (nb * 255.0).round() as u8;
    }
}

// Very rough "smart blur": strong blur, then mix based on local contrast threshold.
fn apply_smart_blur(img: &mut RgbaImage, radius: f32, threshold: f32) {
    if radius <= 0.0 {
        return;
    }
    let blurred = apply_gaussian_blur(img, radius);
    let (w, h) = img.dimensions();
    let mut out = img.clone();

    // Threshold is 0–100; normalize
    let t = threshold / 100.0;

    for y in 0..h {
        for x in 0..w {
            let orig = img.get_pixel(x, y);
            let b = blurred.get_pixel(x, y);

            let dr = (orig[0] as f32 - b[0] as f32).abs() / 255.0;
            let dg = (orig[1] as f32 - b[1] as f32).abs() / 255.0;
            let db = (orig[2] as f32 - b[2] as f32).abs() / 255.0;
            let edge = (dr + dg + db) / 3.0;

            // If below threshold, take more blur; if above, keep more original.
            let blend = if edge < t { 0.8 } else { 0.2 };
            let mut res = [0u8; 4];

            for c in 0..3 {
                let o = orig[c] as f32;
                let bb = b[c] as f32;
                let v = o * (1.0 - blend) + bb * blend;
                res[c] = v.round().clamp(0.0, 255.0) as u8;
            }
            res[3] = orig[3];
            out.put_pixel(x, y, Rgba(res));
        }
    }

    *img = out;
}

// -------------- MASTER PIPELINE --------------

fn process_image(img: DynamicImage, params: &BrParameters) -> RgbaImage {
    let mut rgba = img.to_rgba8();

    // 1. CURVE (luma)
    apply_curve_luma(&mut rgba, params);

    // 2. GAUSS BLUR
    if params.gauss_blur_radius > 0.0 {
        rgba = apply_gaussian_blur(&rgba, params.gauss_blur_radius);
    }

    // 3. EXPOSURE + 4. GAMMA
    apply_exposure_and_gamma(&mut rgba, params);

    // 5. UNSHARP
    rgba = apply_unsharp_mask(
        &rgba,
        params.unsharp_radius,
        params.unsharp_percent,
        params.unsharp_threshold,
    );

    // 6. REDUCE NOISE
    apply_noise_reduction(&mut rgba, params.reduce_noise_strength);

    // 7. BRIGHTNESS + 8. CONTRAST
    apply_brightness_contrast(&mut rgba, params.brightness_factor, params.contrast_factor);

    // 9. HUE + 10. SAT + 11. LIGHTNESS
    apply_hsl_adjustments(&mut rgba, params);

    // 12. VIBRANCE
    apply_vibrance(&mut rgba, params);

    // LOW RES EXTRAS: SMART BLUR (Z14/Z13)
    if let Some(radius) = params.smart_blur_radius {
        let thr = params.smart_blur_threshold.unwrap_or(70.0);
        apply_smart_blur(&mut rgba, radius, thr);
    }

    rgba
}

fn detect_group(group_arg: Option<String>, zoom: Option<u8>) -> &'static str {
    if let Some(g) = group_arg {
        let g = g.to_lowercase();
        return match g.as_str() {
            "high" => "high",
            "mid" => "mid",
            "z14" => "z14",
            "z13" | "heaven" => "z13",
            _ => {
                eprintln!("Unknown group '{}', defaulting to 'high'", g);
                "high"
            }
        };
    }

    if let Some(z) = zoom {
        return match z {
            17 | 18 | 19 => "high",
            15 | 16 => "mid",
            14 => "z14",
            13 => "z13",
            _ => {
                eprintln!("Zoom {} not mapped, defaulting to 'high'", z);
                "high"
            }
        };
    }

    "high"
}

fn params_for_group(group: &str) -> BrParameters {
    match group {
        "high" => params_high(),
        "mid" => params_mid(),
        "z14" => params_z14(),
        "z13" => params_z13(),
        _ => params_high(),
    }
}

fn is_image_file(path: &Path) -> bool {
    if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
        let e = ext.to_lowercase();
        matches!(e.as_str(), "png" | "jpg" | "jpeg")
    } else {
        false
    }
}

// -------------- MAIN --------------

fn main() {
    let cli = Cli::parse();
    let timer = StepTimer::new(3, "3_step_rust_brcj", cli.output.clone());

    if let Some(w) = cli.workers {
        rayon::ThreadPoolBuilder::new()
            .num_threads(w)
            .build_global()
            .expect("Failed to set Rayon thread count");
    }

    let group = detect_group(cli.group.clone(), cli.zoom);
    let params = params_for_group(group);

    println!(
        "BRCJ Rust: group='{}' (zoom {:?}) | input='{}' | output='{}'",
        group,
        cli.zoom,
        cli.input.display(),
        cli.output.display()
    );

    fs::create_dir_all(&cli.output).expect("Failed to create output directory");

    let mut files: Vec<PathBuf> = Vec::new();
    for entry in WalkDir::new(&cli.input)
        .follow_links(true)
        .into_iter()
        .filter_map(Result::ok)
    {
        if entry.file_type().is_file() && is_image_file(entry.path()) {
            files.push(entry.path().to_path_buf());
        }
    }

    let pb = ProgressBar::new(files.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {pos}/{len} ({eta})",
        )
        .unwrap()
        .progress_chars("#>-"),
    );
    let progress = ProgressReporter::new(3, "3_step_rust_brcj", files.len().max(1));
    progress.start(Some("Processing BRCJ tiles".to_string()));
    let completed = AtomicUsize::new(0);
    let errors = AtomicUsize::new(0);

    files.par_iter().for_each(|src_path| {
        let rel = src_path.strip_prefix(&cli.input).unwrap_or(src_path);
        let out_path = cli.output.join(rel);

        if !cli.overwrite && out_path.exists() {
            pb.inc(1);
            let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
            if done == files.len() || done % 250 == 0 {
                progress.update(
                    done,
                    Some(files.len().max(1)),
                    errors.load(Ordering::Relaxed),
                    Some("Processing BRCJ tiles".to_string()),
                );
            }
            return;
        }

        if let Some(parent) = out_path.parent() {
            if let Err(err) = fs::create_dir_all(parent) {
                eprintln!("Failed to create dir {}: {}", parent.display(), err);
                errors.fetch_add(1, Ordering::Relaxed);
                pb.inc(1);
                let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
                if done == files.len() || done % 250 == 0 {
                    progress.update(
                        done,
                        Some(files.len().max(1)),
                        errors.load(Ordering::Relaxed),
                        Some("Processing BRCJ tiles".to_string()),
                    );
                }
                return;
            }
        }

        // Decode by sniffing bytes, not extension (handles JPEG named .png)
        let img_result: Result<DynamicImage, image::ImageError> = (|| {
            let r = ImageReader::open(src_path)?;
            let r = r.with_guessed_format()?;
            let img = r.decode()?;
            Ok(img)
        })();

        match img_result {
            Ok(img) => {
                let processed = process_image(img, &params);
                if let Err(err) = processed.save(&out_path) {
                    eprintln!("Failed to save {}: {}", out_path.display(), err);
                    errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            Err(err) => {
                eprintln!("Failed to open {}: {}", src_path.display(), err);
                errors.fetch_add(1, Ordering::Relaxed);
            }
        }

        pb.inc(1);
        let done = completed.fetch_add(1, Ordering::Relaxed) + 1;
        if done == files.len() || done % 250 == 0 {
            progress.update(
                done,
                Some(files.len().max(1)),
                errors.load(Ordering::Relaxed),
                Some("Processing BRCJ tiles".to_string()),
            );
        }
    });

    pb.finish_with_message("BRCJ pass complete.");
    progress.finish(
        files.len(),
        Some(files.len().max(1)),
        errors.load(Ordering::Relaxed),
        Some("BRCJ pass complete".to_string()),
    );
    let _ = timer.finish(
        Some(files.len()),
        Some(files.len().saturating_sub(errors.load(Ordering::Relaxed))),
        Some(errors.load(Ordering::Relaxed)),
        format!(
            "Processed {} tiles into {}",
            files.len(),
            cli.output.display()
        ),
    );
}
