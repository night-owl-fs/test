use std::path::Path;

const WEBM_HALF: f64 = 20_037_508.342_789_244;
const WEBM_WORLD: f64 = WEBM_HALF * 2.0;
const TILE_SIZE: f64 = 256.0;

pub fn parse_xy_and_zoom_from_path(path: &Path, forced_zoom: Option<u8>) -> Option<(u8, u32, u32)> {
    let stem = path.file_stem()?.to_str()?;
    let parts = stem.split('_').collect::<Vec<_>>();

    let (zoom_from_name, x, y) = if parts.len() == 3 && parts[0].starts_with('Z') {
        let z = parts[0].trim_start_matches('Z').parse::<u8>().ok()?;
        let x = parts[1].parse::<u32>().ok()?;
        let y = parts[2].parse::<u32>().ok()?;
        (Some(z), x, y)
    } else if parts.len() == 2 {
        let x = parts[0].parse::<u32>().ok()?;
        let y = parts[1].parse::<u32>().ok()?;
        (None, x, y)
    } else if parts.len() == 1 {
        // Support standard XYZ layout: .../<z>/<x>/<y>.png
        let y = parts[0].parse::<u32>().ok()?;
        let x = path
            .parent()?
            .file_name()?
            .to_str()?
            .parse::<u32>()
            .ok()?;
        let z = path
            .parent()?
            .parent()?
            .file_name()?
            .to_str()?
            .parse::<u8>()
            .ok()?;
        (Some(z), x, y)
    } else {
        return None;
    };

    let zoom = forced_zoom
        .or(zoom_from_name)
        .or_else(|| path.parent()?.file_name()?.to_str()?.parse::<u8>().ok())?;

    Some((zoom, x, y))
}

pub fn worldfile_text(zoom: u8, x: u32, y: u32) -> String {
    let n = 1u64 << zoom;
    let res = WEBM_WORLD / ((n as f64) * TILE_SIZE);
    let minx = -WEBM_HALF + (x as f64) * TILE_SIZE * res;
    let maxy = WEBM_HALF - (y as f64) * TILE_SIZE * res;
    let c = minx + res / 2.0;
    let f = maxy - res / 2.0;

    format!(
        "{res:.12}\n0.000000000000\n0.000000000000\n{neg_res:.12}\n{c:.12}\n{f:.12}\n",
        res = res,
        neg_res = -res,
        c = c,
        f = f
    )
}

pub fn is_supported_tile(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|ext| {
            let ext = ext.to_ascii_lowercase();
            ext == "png" || ext == "jpg" || ext == "jpeg"
        })
        .unwrap_or(false)
}
