use image::{imageops, imageops::FilterType, RgbImage};

use crate::TileCoord;

pub const UPSAMPLED_PARENT_SIZE: u32 = 512;
pub const CHILD_TILE_SIZE: u32 = UPSAMPLED_PARENT_SIZE / 2;

pub fn split_512_to_children(img: &RgbImage) -> Option<[RgbImage; 4]> {
    if img.width() != UPSAMPLED_PARENT_SIZE || img.height() != UPSAMPLED_PARENT_SIZE {
        return None;
    }

    let nw = imageops::crop_imm(img, 0, 0, CHILD_TILE_SIZE, CHILD_TILE_SIZE).to_image();
    let ne = imageops::crop_imm(img, CHILD_TILE_SIZE, 0, CHILD_TILE_SIZE, CHILD_TILE_SIZE)
        .to_image();
    let sw = imageops::crop_imm(img, 0, CHILD_TILE_SIZE, CHILD_TILE_SIZE, CHILD_TILE_SIZE)
        .to_image();
    let se = imageops::crop_imm(
        img,
        CHILD_TILE_SIZE,
        CHILD_TILE_SIZE,
        CHILD_TILE_SIZE,
        CHILD_TILE_SIZE,
    )
    .to_image();

    Some([nw, ne, sw, se])
}

pub fn child_tiles(tile: TileCoord) -> [TileCoord; 4] {
    [
        TileCoord {
            z: tile.z + 1,
            x: tile.x * 2,
            y: tile.y * 2,
        },
        TileCoord {
            z: tile.z + 1,
            x: tile.x * 2 + 1,
            y: tile.y * 2,
        },
        TileCoord {
            z: tile.z + 1,
            x: tile.x * 2,
            y: tile.y * 2 + 1,
        },
        TileCoord {
            z: tile.z + 1,
            x: tile.x * 2 + 1,
            y: tile.y * 2 + 1,
        },
    ]
}

pub fn descendant_from_ancestor(
    ancestor: &RgbImage,
    source: TileCoord,
    target: TileCoord,
    filter: FilterType,
) -> Option<RgbImage> {
    if target.z <= source.z {
        return None;
    }

    let delta = target.z - source.z;
    if source.x != (target.x >> delta) || source.y != (target.y >> delta) {
        return None;
    }

    let mut current = ancestor.clone();
    let mut current_tile = source;
    for shift in (0..delta).rev() {
        let child_x = (target.x >> shift) & 1;
        let child_y = (target.y >> shift) & 1;
        let child_index = ((child_y & 1) * 2 + (child_x & 1)) as usize;
        current_tile = child_tiles(current_tile)[child_index];
        current = select_child(&current, child_x, child_y, filter)?;
    }

    debug_assert_eq!(current_tile, target);
    Some(current)
}

fn select_child(
    parent: &RgbImage,
    child_x: u32,
    child_y: u32,
    filter: FilterType,
) -> Option<RgbImage> {
    let upsampled = imageops::resize(parent, UPSAMPLED_PARENT_SIZE, UPSAMPLED_PARENT_SIZE, filter);
    let [nw, ne, sw, se] = split_512_to_children(&upsampled)?;

    match (child_x & 1, child_y & 1) {
        (0, 0) => Some(nw),
        (1, 0) => Some(ne),
        (0, 1) => Some(sw),
        (1, 1) => Some(se),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use image::{Rgb, RgbImage};

    use super::{child_tiles, descendant_from_ancestor, split_512_to_children, CHILD_TILE_SIZE};
    use crate::TileCoord;

    #[test]
    fn child_order_matches_xyz_quadrants() {
        let tile = TileCoord { z: 10, x: 4, y: 7 };
        assert_eq!(
            child_tiles(tile),
            [
                TileCoord { z: 11, x: 8, y: 14 },
                TileCoord { z: 11, x: 9, y: 14 },
                TileCoord { z: 11, x: 8, y: 15 },
                TileCoord { z: 11, x: 9, y: 15 },
            ]
        );
    }

    #[test]
    fn split_512_keeps_quadrant_positions() {
        let mut img = RgbImage::new(512, 512);
        for y in 0..512 {
            for x in 0..512 {
                let px = match (x >= 256, y >= 256) {
                    (false, false) => Rgb([255, 0, 0]),
                    (true, false) => Rgb([0, 255, 0]),
                    (false, true) => Rgb([0, 0, 255]),
                    (true, true) => Rgb([255, 255, 0]),
                };
                img.put_pixel(x, y, px);
            }
        }

        let [nw, ne, sw, se] = split_512_to_children(&img).expect("expected 512 split");
        assert_eq!(nw.dimensions(), (CHILD_TILE_SIZE, CHILD_TILE_SIZE));
        assert_eq!(ne.dimensions(), (CHILD_TILE_SIZE, CHILD_TILE_SIZE));
        assert_eq!(sw.dimensions(), (CHILD_TILE_SIZE, CHILD_TILE_SIZE));
        assert_eq!(se.dimensions(), (CHILD_TILE_SIZE, CHILD_TILE_SIZE));
        assert_eq!(nw.get_pixel(32, 32).0, [255, 0, 0]);
        assert_eq!(ne.get_pixel(32, 32).0, [0, 255, 0]);
        assert_eq!(sw.get_pixel(32, 32).0, [0, 0, 255]);
        assert_eq!(se.get_pixel(32, 32).0, [255, 255, 0]);
    }

    #[test]
    fn descendant_upsample_returns_child_tile_size() {
        let mut img = RgbImage::new(256, 256);
        for y in 0..256 {
            for x in 0..256 {
                img.put_pixel(x, y, Rgb([x as u8, y as u8, ((x + y) / 2) as u8]));
            }
        }

        let out = descendant_from_ancestor(
            &img,
            TileCoord { z: 10, x: 3, y: 5 },
            TileCoord { z: 12, x: 14, y: 23 },
            image::imageops::FilterType::CatmullRom,
        )
        .expect("expected descendant");

        assert_eq!(out.dimensions(), (256, 256));
    }
}
