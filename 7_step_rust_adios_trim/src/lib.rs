use std::io::Cursor;

use anyhow::Result;
use image::codecs::jpeg::JpegEncoder;
use image::DynamicImage;

pub fn table_has_tile_schema(columns: &[String]) -> bool {
    ["zoom_level", "tile_column", "tile_row", "tile_data"]
        .iter()
        .all(|required| columns.iter().any(|c| c == required))
}

pub fn encode_jpeg(bytes: &[u8], quality: u8) -> Result<Vec<u8>> {
    let img = image::load_from_memory(bytes)?;
    let mut out = Vec::new();
    {
        let mut encoder = JpegEncoder::new_with_quality(Cursor::new(&mut out), quality);
        encoder.encode_image(&img)?;
    }
    Ok(out)
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
