use std::path::{Path, PathBuf};

use pipeline_core::{expand_job_to_tiles, ConeJob};

pub fn tile_path_from_job(tiles_root: &Path, job: &ConeJob, ext: &str) -> Vec<PathBuf> {
    let ext = ext.trim_start_matches('.');
    expand_job_to_tiles(job)
        .into_iter()
        .map(|tile| {
            tiles_root
                .join(&tile.icao)
                .join(tile.z.to_string())
                .join(tile.x.to_string())
                .join(format!("{}.{}", tile.y, ext))
        })
        .collect()
}
