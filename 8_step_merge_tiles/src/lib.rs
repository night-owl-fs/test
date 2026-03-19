use std::io::Cursor;

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

pub fn is_solid_background_tile(bytes: &[u8], rgb: [u8; 3]) -> bool {
    let reader = Cursor::new(bytes);
    let Ok(img) = image::load(reader, image::ImageFormat::Jpeg) else {
        return false;
    };
    let rgb_img = img.to_rgb8();
    rgb_img
        .pixels()
        .all(|p| p[0] == rgb[0] && p[1] == rgb[1] && p[2] == rgb[2])
}
