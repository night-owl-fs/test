use std::path::{Path, PathBuf};

use anyhow::{anyhow, Result};
use walkdir::WalkDir;

pub fn discover_geotiffs(input: &Path, recursive: bool) -> Vec<PathBuf> {
    let mut files = Vec::new();
    if recursive {
        for entry in WalkDir::new(input).into_iter().filter_map(Result::ok) {
            if !entry.file_type().is_file() {
                continue;
            }
            let path = entry.path();
            if path
                .extension()
                .and_then(|x| x.to_str())
                .map(|ext| {
                    let ext = ext.to_ascii_lowercase();
                    ext == "tif" || ext == "tiff"
                })
                .unwrap_or(false)
            {
                files.push(path.to_path_buf());
            }
        }
    } else if let Ok(read_dir) = std::fs::read_dir(input) {
        for entry in read_dir.filter_map(Result::ok) {
            let path = entry.path();
            if !path.is_file() {
                continue;
            }
            if path
                .extension()
                .and_then(|x| x.to_str())
                .map(|ext| {
                    let ext = ext.to_ascii_lowercase();
                    ext == "tif" || ext == "tiff"
                })
                .unwrap_or(false)
            {
                files.push(path);
            }
        }
    }
    files.sort();
    files
}

pub fn render_command(command: &[String]) -> String {
    command
        .iter()
        .map(|p| {
            if p.contains(' ') {
                format!("\"{p}\"")
            } else {
                p.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

pub fn validate_sqlite_output_path(output: &Path) -> Result<()> {
    match output.extension().and_then(|ext| ext.to_str()) {
        Some(ext) if ext.eq_ignore_ascii_case("sqlite") => Ok(()),
        Some(ext) => Err(anyhow!(
            "output_db must end with .sqlite for Cesium imagery tiler exports (got .{ext})"
        )),
        None => Err(anyhow!(
            "output_db must end with .sqlite for Cesium imagery tiler exports"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::validate_sqlite_output_path;
    use std::path::Path;

    #[test]
    fn accepts_sqlite_output_path() {
        assert!(validate_sqlite_output_path(Path::new("imagery.sqlite")).is_ok());
    }

    #[test]
    fn rejects_gpkg_output_path() {
        let err = validate_sqlite_output_path(Path::new("imagery.gpkg")).unwrap_err();
        assert!(err.to_string().contains(".sqlite"));
    }

    #[test]
    fn rejects_extensionless_output_path() {
        let err = validate_sqlite_output_path(Path::new("imagery")).unwrap_err();
        assert!(err.to_string().contains(".sqlite"));
    }
}
