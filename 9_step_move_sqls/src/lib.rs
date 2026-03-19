use std::path::{Path, PathBuf};

pub fn is_sql_artifact(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "sqlite" || ext == "db" || ext == "gpkg" || ext == "mbtiles"
        })
        .unwrap_or(false)
}

pub fn unique_target_path(target_dir: &Path, file_name: &str) -> PathBuf {
    let initial = target_dir.join(file_name);
    if !initial.exists() {
        return initial;
    }

    let path = Path::new(file_name);
    let stem = path
        .file_stem()
        .and_then(|x| x.to_str())
        .unwrap_or("artifact");
    let ext = path.extension().and_then(|x| x.to_str()).unwrap_or("");

    for i in 1..10_000 {
        let candidate = if ext.is_empty() {
            format!("{stem}_{i}")
        } else {
            format!("{stem}_{i}.{ext}")
        };
        let full = target_dir.join(candidate);
        if !full.exists() {
            return full;
        }
    }
    target_dir.join(file_name)
}
