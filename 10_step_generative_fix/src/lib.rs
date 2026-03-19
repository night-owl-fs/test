use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use walkdir::WalkDir;

#[derive(Debug, Clone)]
pub struct TileCoord {
    pub z: u32,
    pub x: u32,
    pub y: u32,
}

pub fn discover_xyz_tiles(root: &Path) -> BTreeMap<u32, BTreeSet<(u32, u32)>> {
    let mut by_zoom: BTreeMap<u32, BTreeSet<(u32, u32)>> = BTreeMap::new();
    for entry in WalkDir::new(root).into_iter().filter_map(Result::ok) {
        if !entry.file_type().is_file() {
            continue;
        }
        let path = entry.path();
        if path
            .extension()
            .and_then(|x| x.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("png") || ext.eq_ignore_ascii_case("jpg"))
            .unwrap_or(false)
        {
            let y = path
                .file_stem()
                .and_then(|x| x.to_str())
                .and_then(|s| s.parse::<u32>().ok());
            let x = path
                .parent()
                .and_then(|p| p.file_name())
                .and_then(|x| x.to_str())
                .and_then(|s| s.parse::<u32>().ok());
            let z = path
                .parent()
                .and_then(|p| p.parent())
                .and_then(|p| p.file_name())
                .and_then(|x| x.to_str())
                .and_then(|s| s.parse::<u32>().ok());
            if let (Some(z), Some(x), Some(y)) = (z, x, y) {
                by_zoom.entry(z).or_default().insert((x, y));
            }
        }
    }
    by_zoom
}
