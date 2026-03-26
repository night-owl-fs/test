use std::io::Cursor;

use anyhow::Result;
use image::codecs::jpeg::JpegEncoder;
use image::{DynamicImage, Rgb, RgbImage};

pub fn table_has_tile_schema(columns: &[String]) -> bool {
    ["zoom_level", "tile_column", "tile_row", "tile_data"]
        .iter()
        .all(|required| columns.iter().any(|c| c == required))
}

pub fn parse_rgb(text: &str) -> Option<[u8; 3]> {
    let parts = text.split(',').collect::<Vec<_>>();
    if parts.len() != 3 {
        return None;
    }
    let r = parts[0].trim().parse::<u8>().ok()?;
    let g = parts[1].trim().parse::<u8>().ok()?;
    let b = parts[2].trim().parse::<u8>().ok()?;
    Some([r, g, b])
}

pub fn encode_jpeg(
    bytes: &[u8],
    quality: u8,
    background: [u8; 3],
    skip_fully_transparent: bool,
    skip_any_alpha: bool,
) -> Result<Option<Vec<u8>>> {
    let img = image::load_from_memory(bytes)?;
    let rgb = flatten_alpha(&img, background, skip_fully_transparent, skip_any_alpha)?;

    let Some(rgb) = rgb else {
        return Ok(None);
    };

    let mut out = Vec::new();
    {
        let mut encoder = JpegEncoder::new_with_quality(Cursor::new(&mut out), quality);
        encoder.encode_image(&DynamicImage::ImageRgb8(rgb))?;
    }
    Ok(Some(out))
}

pub fn has_transparency(bytes: &[u8]) -> bool {
    match image::load_from_memory(bytes) {
        Ok(img) => image_has_transparency(&img),
        Err(_) => false,
    }
}

fn image_has_transparency(img: &DynamicImage) -> bool {
    let rgba = img.to_rgba8();
    rgba.pixels().any(|p| p[3] < 255)
}

fn flatten_alpha(
    img: &DynamicImage,
    background: [u8; 3],
    skip_fully_transparent: bool,
    skip_any_alpha: bool,
) -> Result<Option<RgbImage>> {
    let rgba = img.to_rgba8();
    let has_alpha = rgba.pixels().any(|p| p[3] < 255);

    if !has_alpha {
        return Ok(Some(img.to_rgb8()));
    }

    if skip_any_alpha {
        return Ok(None);
    }

    let fully_transparent = rgba.pixels().all(|p| p[3] == 0);
    if fully_transparent && skip_fully_transparent {
        return Ok(None);
    }

    let mut out = RgbImage::new(rgba.width(), rgba.height());
    for (x, y, pixel) in rgba.enumerate_pixels() {
        let alpha = pixel[3] as f32 / 255.0;
        let inv = 1.0 - alpha;
        let rr = pixel[0] as f32 * alpha + background[0] as f32 * inv;
        let gg = pixel[1] as f32 * alpha + background[1] as f32 * inv;
        let bb = pixel[2] as f32 * alpha + background[2] as f32 * inv;
        out.put_pixel(
            x,
            y,
            Rgb([rr.round() as u8, gg.round() as u8, bb.round() as u8]),
        );
    }
    Ok(Some(out))
}
