use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MoveRecord {
    pub source: String,
    pub target: String,
    pub action: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DestinationConfig {
    pub default_destination: String,
    pub updated_unix_secs: u64,
}

pub fn is_sql_artifact(path: &Path) -> bool {
    path.extension()
        .and_then(|e| e.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "sqlite" || ext == "db" || ext == "gpkg" || ext == "mbtiles"
        })
        .unwrap_or(false)
}

pub fn default_destination_config_path() -> PathBuf {
    let home = env::var_os("HOME")
        .or_else(|| env::var_os("USERPROFILE"))
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."));
    home.join(".beavery").join("step9_destination.json")
}

pub fn fallback_output_dir(input_dir: &Path) -> PathBuf {
    input_dir
        .parent()
        .map(|parent| parent.join("final"))
        .unwrap_or_else(|| input_dir.join("final"))
}

pub fn load_destination_config(config_path: &Path) -> Result<Option<DestinationConfig>> {
    if !config_path.exists() {
        return Ok(None);
    }
    let raw = fs::read_to_string(config_path).with_context(|| {
        format!(
            "Failed to read destination config {}",
            config_path.display()
        )
    })?;
    let config = serde_json::from_str::<DestinationConfig>(&raw).with_context(|| {
        format!(
            "Failed to parse destination config {}",
            config_path.display()
        )
    })?;
    Ok(Some(config))
}

pub fn clear_destination_config(config_path: &Path) -> Result<()> {
    if config_path.exists() {
        fs::remove_file(config_path)
            .with_context(|| format!("Failed to remove {}", config_path.display()))?;
    }
    Ok(())
}

pub fn save_destination_config(
    config_path: &Path,
    destination: &Path,
) -> Result<DestinationConfig> {
    let destination = absolutize(destination)?;
    let config = DestinationConfig {
        default_destination: destination.display().to_string(),
        updated_unix_secs: now_unix_secs(),
    };
    if let Some(parent) = config_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(config_path, serde_json::to_string_pretty(&config)?)
        .with_context(|| format!("Failed to write {}", config_path.display()))?;
    Ok(config)
}

pub fn resolve_output_dir(
    input_dir: &Path,
    explicit_output_dir: Option<&Path>,
    set_default_destination: Option<&Path>,
    config_path: &Path,
) -> Result<PathBuf> {
    if let Some(path) = set_default_destination {
        save_destination_config(config_path, path)?;
        return absolutize(path);
    }
    if let Some(path) = explicit_output_dir {
        save_destination_config(config_path, path)?;
        return absolutize(path);
    }
    if let Some(config) = load_destination_config(config_path)? {
        return Ok(PathBuf::from(config.default_destination));
    }
    Ok(fallback_output_dir(input_dir))
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

fn now_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn absolutize(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}
