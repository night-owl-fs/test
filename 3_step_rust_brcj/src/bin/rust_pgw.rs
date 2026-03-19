use clap::Parser;
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

const WEBM_HALF: f64 = 20037508.342789244;
const WEBM_WORLD: f64 = WEBM_HALF * 2.0;
const TILE_SIZE: f64 = 256.0;

#[derive(Parser, Debug)]
#[command(author, version, about = "Generate PGW sidecars for x_y.png tiles")]
struct Cli {
    /// Root folder to scan for PNG tiles
    #[arg(short = 'r', long)]
    root: PathBuf,

    /// Explicit zoom level (if not set, use numeric parent folder name)
    #[arg(long)]
    zoom: Option<u8>,

    /// Recursively scan root folder
    #[arg(long, default_value_t = true)]
    recursive: bool,

    /// Also write .png.pgw sidecars
    #[arg(long, default_value_t = true)]
    write_png_pgw: bool,

    /// Overwrite existing sidecars
    #[arg(long)]
    overwrite: bool,
}

fn parse_xy_from_stem(path: &Path) -> Option<(u32, u32)> {
    let stem = path.file_stem()?.to_str()?;
    let mut parts = stem.split('_');
    let x = parts.next()?.parse::<u32>().ok()?;
    let y = parts.next()?.parse::<u32>().ok()?;
    if parts.next().is_some() {
        return None;
    }
    Some((x, y))
}

fn infer_zoom(path: &Path) -> Option<u8> {
    let parent = path.parent()?;
    let name = parent.file_name()?.to_str()?;
    name.parse::<u8>().ok()
}

fn worldfile_text(zoom: u8, x: u32, y: u32) -> String {
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

fn should_process_png(path: &Path) -> bool {
    path.extension()
        .and_then(|x| x.to_str())
        .map(|x| x.eq_ignore_ascii_case("png"))
        .unwrap_or(false)
}

fn main() {
    let cli = Cli::parse();
    let root = cli.root;

    if !root.exists() {
        eprintln!("Root does not exist: {}", root.display());
        std::process::exit(2);
    }

    let mut total_png = 0usize;
    let mut written = 0usize;
    let mut skipped_existing = 0usize;
    let mut skipped_bad_name = 0usize;
    let mut skipped_no_zoom = 0usize;
    let mut errors = 0usize;

    if cli.recursive {
        for entry in WalkDir::new(&root).follow_links(true).into_iter().filter_map(Result::ok) {
            let p = entry.path();
            if !entry.file_type().is_file() || !should_process_png(p) {
                continue;
            }
            total_png += 1;
            let (x, y) = match parse_xy_from_stem(p) {
                Some(v) => v,
                None => {
                    skipped_bad_name += 1;
                    continue;
                }
            };
            let z = match cli.zoom.or_else(|| infer_zoom(p)) {
                Some(v) => v,
                None => {
                    skipped_no_zoom += 1;
                    continue;
                }
            };

            let text = worldfile_text(z, x, y);
            let pgw = p.with_extension("pgw");
            if pgw.exists() && !cli.overwrite {
                skipped_existing += 1;
            } else if let Err(err) = fs::write(&pgw, text.as_bytes()) {
                eprintln!("Write failed {}: {}", pgw.display(), err);
                errors += 1;
                continue;
            } else {
                written += 1;
            }

            if cli.write_png_pgw {
                let png_pgw = PathBuf::from(format!("{}.pgw", p.display()));
                if png_pgw.exists() && !cli.overwrite {
                    // no-op
                } else if let Err(err) = fs::write(&png_pgw, text.as_bytes()) {
                    eprintln!("Write failed {}: {}", png_pgw.display(), err);
                    errors += 1;
                }
            }
        }
    } else {
        let entries = match fs::read_dir(&root) {
            Ok(v) => v,
            Err(err) => {
                eprintln!("Failed to read root {}: {}", root.display(), err);
                std::process::exit(2);
            }
        };
        for entry in entries.filter_map(Result::ok) {
            let p = entry.path();
            if !p.is_file() || !should_process_png(&p) {
                continue;
            }
            total_png += 1;
            let (x, y) = match parse_xy_from_stem(&p) {
                Some(v) => v,
                None => {
                    skipped_bad_name += 1;
                    continue;
                }
            };
            let z = match cli.zoom.or_else(|| infer_zoom(&p)) {
                Some(v) => v,
                None => {
                    skipped_no_zoom += 1;
                    continue;
                }
            };

            let text = worldfile_text(z, x, y);
            let pgw = p.with_extension("pgw");
            if pgw.exists() && !cli.overwrite {
                skipped_existing += 1;
            } else if let Err(err) = fs::write(&pgw, text.as_bytes()) {
                eprintln!("Write failed {}: {}", pgw.display(), err);
                errors += 1;
                continue;
            } else {
                written += 1;
            }

            if cli.write_png_pgw {
                let png_pgw = PathBuf::from(format!("{}.pgw", p.display()));
                if png_pgw.exists() && !cli.overwrite {
                    // no-op
                } else if let Err(err) = fs::write(&png_pgw, text.as_bytes()) {
                    eprintln!("Write failed {}: {}", png_pgw.display(), err);
                    errors += 1;
                }
            }
        }
    }

    println!("RUST_PGW summary");
    println!("root={}", root.display());
    println!("total_png={}", total_png);
    println!("written_pgw={}", written);
    println!("skipped_existing={}", skipped_existing);
    println!("skipped_bad_name={}", skipped_bad_name);
    println!("skipped_no_zoom={}", skipped_no_zoom);
    println!("errors={}", errors);
}
