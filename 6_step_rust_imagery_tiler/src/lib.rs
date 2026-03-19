use std::path::{Path, PathBuf};

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
