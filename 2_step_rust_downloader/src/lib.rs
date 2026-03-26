use std::fs;
use std::io::{Cursor, ErrorKind};
use std::path::Path;

use anyhow::{Context, Result};
use image::ImageFormat;
use pipeline_core::DownloadManifestItem;
use rayon::prelude::*;

pub use pipeline_core::{build_download_manifest, expand_job_to_tiles, ConeJob, DownloadManifest};

#[derive(Debug, Clone, Default)]
pub struct PngNormalizationReport {
    pub converted: usize,
    pub missing: usize,
    pub failed: usize,
    pub sample_errors: Vec<String>,
}

enum PngNormalizationOutcome {
    Converted,
    Missing(String),
    Failed(String),
}

pub fn convert_image_bytes_to_png(bytes: &[u8]) -> Result<Vec<u8>> {
    let image = image::load_from_memory(bytes).context("image decode failed")?;
    let mut output = Cursor::new(Vec::new());
    image
        .write_to(&mut output, ImageFormat::Png)
        .context("PNG encode failed")?;
    Ok(output.into_inner())
}

pub fn write_png_file(path: &Path, bytes: &[u8]) -> Result<()> {
    let png_bytes = convert_image_bytes_to_png(bytes)
        .with_context(|| format!("Failed to convert {} into PNG", path.display()))?;
    fs::write(path, png_bytes)
        .with_context(|| format!("Failed to write PNG file {}", path.display()))?;
    Ok(())
}

pub fn normalize_download_manifest_to_png(
    root: &Path,
    manifest: &DownloadManifest,
) -> PngNormalizationReport {
    let outcomes = manifest
        .items
        .par_iter()
        .map(|item| normalize_manifest_item(root, item))
        .collect::<Vec<_>>();

    let mut report = PngNormalizationReport::default();
    for outcome in outcomes {
        match outcome {
            PngNormalizationOutcome::Converted => report.converted += 1,
            PngNormalizationOutcome::Missing(message) => {
                report.missing += 1;
                if report.sample_errors.len() < 10 {
                    report.sample_errors.push(message);
                }
            }
            PngNormalizationOutcome::Failed(message) => {
                report.failed += 1;
                if report.sample_errors.len() < 10 {
                    report.sample_errors.push(message);
                }
            }
        }
    }
    report
}

fn normalize_manifest_item(root: &Path, item: &DownloadManifestItem) -> PngNormalizationOutcome {
    let path = root.join(&item.relative_path);
    let bytes = match fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == ErrorKind::NotFound => {
            return PngNormalizationOutcome::Missing(format!("Missing file: {}", path.display()));
        }
        Err(error) => {
            return PngNormalizationOutcome::Failed(format!(
                "Failed to read {}: {error}",
                path.display()
            ));
        }
    };

    match write_png_file(&path, &bytes) {
        Ok(()) => PngNormalizationOutcome::Converted,
        Err(error) => PngNormalizationOutcome::Failed(format!(
            "Failed to normalize {}: {error:#}",
            path.display()
        )),
    }
}
