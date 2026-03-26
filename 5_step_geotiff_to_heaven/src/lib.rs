use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Result};
use pipeline_core::{expand_job_to_tiles, ConeJob, TileAddress};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConePattern {
    Grid3x3,
    Grid5x5,
    Inner3x3Of5x5,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ZoomRecipe {
    pub zoom: u32,
    pub mercator_level: u32,
    pub pattern: ConePattern,
    pub expected_job_grid: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConeCell {
    pub row: i32,
    pub col: i32,
    pub mercator_x: u32,
    pub mercator_y: u32,
    pub min_x: u32,
    pub max_x: u32,
    pub min_y: u32,
    pub max_y: u32,
}

const ZOOM_RECIPES: [ZoomRecipe; 6] = [
    ZoomRecipe {
        zoom: 13,
        mercator_level: 9,
        pattern: ConePattern::Grid3x3,
        expected_job_grid: 3,
    },
    ZoomRecipe {
        zoom: 14,
        mercator_level: 9,
        pattern: ConePattern::Grid3x3,
        expected_job_grid: 3,
    },
    ZoomRecipe {
        zoom: 15,
        mercator_level: 9,
        pattern: ConePattern::Grid3x3,
        expected_job_grid: 3,
    },
    ZoomRecipe {
        zoom: 16,
        mercator_level: 11,
        pattern: ConePattern::Grid3x3,
        expected_job_grid: 3,
    },
    ZoomRecipe {
        zoom: 17,
        mercator_level: 13,
        pattern: ConePattern::Grid5x5,
        expected_job_grid: 5,
    },
    ZoomRecipe {
        zoom: 18,
        mercator_level: 13,
        pattern: ConePattern::Inner3x3Of5x5,
        expected_job_grid: 3,
    },
];

pub fn zoom_recipe_for_zoom(zoom: u32) -> Option<ZoomRecipe> {
    ZOOM_RECIPES
        .iter()
        .copied()
        .find(|recipe| recipe.zoom == zoom)
}

pub fn expected_output_count(pattern: ConePattern) -> usize {
    match pattern {
        ConePattern::Grid3x3 => 9,
        ConePattern::Grid5x5 => 25,
        ConePattern::Inner3x3Of5x5 => 9,
    }
}

fn pattern_offsets(pattern: ConePattern) -> &'static [i32] {
    match pattern {
        ConePattern::Grid3x3 | ConePattern::Inner3x3Of5x5 => &[-1, 0, 1],
        ConePattern::Grid5x5 => &[-2, -1, 0, 1, 2],
    }
}

pub fn expected_geotiff_output_count_for_job(job: &ConeJob) -> Result<usize> {
    let recipe = zoom_recipe_for_zoom(job.out_z)
        .ok_or_else(|| anyhow!("No Step 5 zoom recipe defined for Z{}", job.out_z))?;

    if job.base_z != recipe.mercator_level {
        bail!(
            "Step 5 recipe mismatch for {} Z{}: expected Mercator level {}, job base_z={}",
            job.icao,
            job.out_z,
            recipe.mercator_level,
            job.base_z
        );
    }

    if job.grid != recipe.expected_job_grid {
        bail!(
            "Step 5 recipe mismatch for {} Z{}: expected grid {}, job grid={}",
            job.icao,
            job.out_z,
            recipe.expected_job_grid,
            job.grid
        );
    }

    Ok(expected_output_count(recipe.pattern))
}

pub fn expected_geotiff_output_count(jobs: &[ConeJob]) -> Result<usize> {
    jobs.iter()
        .map(expected_geotiff_output_count_for_job)
        .sum::<Result<usize>>()
}

pub fn cone_cells_for_job(job: &ConeJob) -> Result<Vec<ConeCell>> {
    let recipe = zoom_recipe_for_zoom(job.out_z)
        .ok_or_else(|| anyhow!("No Step 5 zoom recipe defined for Z{}", job.out_z))?;

    if job.base_z != recipe.mercator_level {
        bail!(
            "Step 5 recipe mismatch for {} Z{}: expected Mercator level {}, job base_z={}",
            job.icao,
            job.out_z,
            recipe.mercator_level,
            job.base_z
        );
    }

    if job.grid != recipe.expected_job_grid {
        bail!(
            "Step 5 recipe mismatch for {} Z{}: expected grid {}, job grid={}",
            job.icao,
            job.out_z,
            recipe.expected_job_grid,
            job.grid
        );
    }

    if job.out_z < job.base_z {
        bail!(
            "Invalid Step 5 job for {} Z{}: out_z is lower than base_z ({})",
            job.icao,
            job.out_z,
            job.base_z
        );
    }

    let factor = 1u32 << (job.out_z - job.base_z);
    let center_index = (job.grid / 2) as i32;
    let mut cells = Vec::new();

    for row in pattern_offsets(recipe.pattern) {
        for col in pattern_offsets(recipe.pattern) {
            let grid_x = center_index + col;
            let grid_y = center_index + row;

            if grid_x < 0 || grid_y < 0 || grid_x >= job.grid as i32 || grid_y >= job.grid as i32 {
                bail!(
                    "Step 5 cone cell out of bounds for {} Z{}: row={}, col={}, grid={}",
                    job.icao,
                    job.out_z,
                    row,
                    col,
                    job.grid
                );
            }

            let mercator_x = job.base_x + grid_x as u32;
            let mercator_y = job.base_y + grid_y as u32;
            let min_x = mercator_x * factor;
            let min_y = mercator_y * factor;

            cells.push(ConeCell {
                row: *row,
                col: *col,
                mercator_x,
                mercator_y,
                min_x,
                max_x: min_x + factor - 1,
                min_y,
                max_y: min_y + factor - 1,
            });
        }
    }

    let expected = expected_output_count(recipe.pattern);
    if cells.len() != expected {
        bail!(
            "Step 5 cone cell mismatch for {} Z{}: expected {}, got {}",
            job.icao,
            job.out_z,
            expected,
            cells.len()
        );
    }

    Ok(cells)
}

pub fn select_tiles_in_cell(job: &ConeJob, cell: &ConeCell) -> Vec<TileAddress> {
    expand_job_to_tiles(job)
        .into_iter()
        .filter(|tile| {
            tile.x >= cell.min_x
                && tile.x <= cell.max_x
                && tile.y >= cell.min_y
                && tile.y <= cell.max_y
        })
        .collect()
}

pub fn tile_paths_for_cell(
    tiles_root: &Path,
    job: &ConeJob,
    cell: &ConeCell,
    ext: &str,
) -> Vec<PathBuf> {
    let ext = ext.trim_start_matches('.');
    select_tiles_in_cell(job, cell)
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

pub fn output_stem(job: &ConeJob, recipe: ZoomRecipe, cell: &ConeCell) -> String {
    format!(
        "{}_Z{}_ML{}_cone_r{}_c{}_fromZ{}",
        job.icao, job.out_z, recipe.mercator_level, cell.row, cell.col, job.out_z
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_job(base_z: u32, base_x: u32, base_y: u32, grid: u32, out_z: u32) -> ConeJob {
        ConeJob {
            icao: "CYVR".to_string(),
            name: "Vancouver Intl".to_string(),
            base_z,
            base_x,
            base_y,
            grid,
            out_z,
        }
    }

    #[test]
    fn expected_counts_match_patterns() {
        assert_eq!(expected_output_count(ConePattern::Grid3x3), 9);
        assert_eq!(expected_output_count(ConePattern::Grid5x5), 25);
        assert_eq!(expected_output_count(ConePattern::Inner3x3Of5x5), 9);
    }

    #[test]
    fn z17_generates_twenty_five_cells() {
        let job = make_job(13, 1290, 2804, 5, 17);
        let cells = cone_cells_for_job(&job).expect("cells");
        assert_eq!(cells.len(), 25);
        assert_eq!(cells.first().map(|c| (c.row, c.col)), Some((-2, -2)));
        assert_eq!(cells.last().map(|c| (c.row, c.col)), Some((2, 2)));
    }

    #[test]
    fn z18_generates_inner_three_by_three_cells() {
        let job = make_job(13, 1291, 2805, 3, 18);
        let cells = cone_cells_for_job(&job).expect("cells");
        assert_eq!(cells.len(), 9);
        assert_eq!(cells.first().map(|c| (c.row, c.col)), Some((-1, -1)));
        assert_eq!(cells.last().map(|c| (c.row, c.col)), Some((1, 1)));
    }

    #[test]
    fn cyvr_jobs_expand_to_seventy_outputs() {
        let jobs = vec![
            make_job(9, 79, 174, 3, 13),
            make_job(9, 79, 174, 3, 14),
            make_job(9, 79, 174, 3, 15),
            make_job(11, 322, 700, 3, 16),
            make_job(13, 1290, 2804, 5, 17),
            make_job(13, 1291, 2805, 3, 18),
        ];

        assert_eq!(expected_geotiff_output_count(&jobs).expect("count"), 70);
    }
}
