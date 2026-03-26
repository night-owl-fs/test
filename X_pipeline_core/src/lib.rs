use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use chrono::Utc;
use rusqlite::{Connection, Row};
use serde::{Deserialize, Serialize};

const MIN_LAT: f64 = -85.05112878;
const MAX_LAT: f64 = 85.05112878;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Airport {
    pub icao: String,
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConeProfile {
    pub base_z: u32,
    pub grid: u32,
    pub out_zooms: Vec<u32>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConeJob {
    pub icao: String,
    pub name: String,
    pub base_z: u32,
    pub base_x: u32,
    pub base_y: u32,
    pub grid: u32,
    pub out_z: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AirportIndexReport {
    pub airports_requested: Vec<String>,
    pub airports_found: Vec<String>,
    pub airports_missing: Vec<String>,
    pub airports: Vec<Airport>,
    pub jobs: Vec<ConeJob>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TileAddress {
    pub icao: String,
    pub z: u32,
    pub x: u32,
    pub y: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadManifestItem {
    pub icao: String,
    pub z: u32,
    pub x: u32,
    pub y: u32,
    pub relative_path: String,
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadManifest {
    pub airport_count: usize,
    pub tile_count: usize,
    pub zoom_counts: BTreeMap<u32, usize>,
    pub items: Vec<DownloadManifestItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProgressState {
    Running,
    Paused,
    Error,
    Complete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BeaveryProgressFrame {
    pub current_step: u32,
    pub step_name: String,
    pub state: ProgressState,
    pub percent: f64,
    pub completed: usize,
    pub total: usize,
    pub rate: Option<f64>,
    pub eta_seconds: Option<u64>,
    pub errors: usize,
    pub timestamp: String,
    pub message: Option<String>,
}

#[derive(Clone)]
pub struct ProgressReporter {
    current_step: u32,
    step_name: Arc<String>,
    total: usize,
    started_at: Instant,
    write_lock: Arc<Mutex<()>>,
}

impl ProgressReporter {
    pub fn new(current_step: u32, step_name: impl Into<String>, total: usize) -> Self {
        Self {
            current_step,
            step_name: Arc::new(step_name.into()),
            total,
            started_at: Instant::now(),
            write_lock: Arc::new(Mutex::new(())),
        }
    }

    pub fn total(&self) -> usize {
        self.total
    }

    pub fn start(&self, message: impl Into<Option<String>>) {
        self.emit(ProgressState::Running, 0, self.total, 0, message);
    }

    pub fn update(
        &self,
        completed: usize,
        total: Option<usize>,
        errors: usize,
        message: impl Into<Option<String>>,
    ) {
        self.emit(
            ProgressState::Running,
            completed,
            total.unwrap_or(self.total),
            errors,
            message,
        );
    }

    pub fn paused(
        &self,
        completed: usize,
        total: Option<usize>,
        errors: usize,
        message: impl Into<Option<String>>,
    ) {
        self.emit(
            ProgressState::Paused,
            completed,
            total.unwrap_or(self.total),
            errors,
            message,
        );
    }

    pub fn finish(
        &self,
        completed: usize,
        total: Option<usize>,
        errors: usize,
        message: impl Into<Option<String>>,
    ) {
        let normalized_total = total.unwrap_or(self.total.max(completed));
        self.emit(
            ProgressState::Complete,
            completed.max(normalized_total),
            normalized_total,
            errors,
            message,
        );
    }

    pub fn fail(
        &self,
        completed: usize,
        total: Option<usize>,
        errors: usize,
        message: impl Into<Option<String>>,
    ) {
        self.emit(
            ProgressState::Error,
            completed,
            total.unwrap_or(self.total.max(completed)),
            errors,
            message,
        );
    }

    pub fn emit(
        &self,
        state: ProgressState,
        completed: usize,
        total: usize,
        errors: usize,
        message: impl Into<Option<String>>,
    ) {
        let normalized_total = total.max(completed);
        let percent = if normalized_total == 0 {
            match state {
                ProgressState::Complete => 100.0,
                _ => 0.0,
            }
        } else {
            ((completed as f64 / normalized_total as f64) * 100.0).clamp(0.0, 100.0)
        };
        let elapsed_seconds = self.started_at.elapsed().as_secs_f64();
        let rate = if elapsed_seconds > 0.0 {
            Some(completed as f64 / elapsed_seconds)
        } else {
            None
        };
        let eta_seconds = match (rate, normalized_total > completed) {
            (Some(value), true) if value > 0.0 => {
                Some(((normalized_total - completed) as f64 / value).ceil() as u64)
            }
            _ => None,
        };
        let frame = BeaveryProgressFrame {
            current_step: self.current_step,
            step_name: (*self.step_name).clone(),
            state,
            percent: (percent * 10.0).round() / 10.0,
            completed,
            total: normalized_total,
            rate: rate.map(|value| (value * 10.0).round() / 10.0),
            eta_seconds,
            errors,
            timestamp: Utc::now().to_rfc3339(),
            message: message.into(),
        };

        if let Ok(serialized) = serde_json::to_string(&frame) {
            let _guard = self.write_lock.lock().ok();
            let mut stdout = io::stdout().lock();
            let _ = writeln!(stdout, "BEAVERY_PROGRESS {serialized}");
            let _ = stdout.flush();
        }
    }
}

pub fn default_cone_profiles() -> Vec<ConeProfile> {
    vec![
        ConeProfile {
            base_z: 9,
            grid: 3,
            out_zooms: vec![13, 14, 15],
        },
        ConeProfile {
            base_z: 11,
            grid: 3,
            out_zooms: vec![16],
        },
        ConeProfile {
            base_z: 13,
            grid: 5,
            out_zooms: vec![17],
        },
        ConeProfile {
            base_z: 13,
            grid: 3,
            out_zooms: vec![18],
        },
    ]
}

pub fn latlon_to_tile(lat_deg: f64, lon_deg: f64, z: u32) -> (u32, u32) {
    let lat = lat_deg.clamp(MIN_LAT, MAX_LAT).to_radians();
    let n = 2f64.powi(z as i32);

    let xtile = ((lon_deg + 180.0) / 360.0 * n).floor();
    let ytile = {
        let tan_term = lat.tan();
        let sec_term = 1.0 / lat.cos();
        ((1.0 - (tan_term + sec_term).ln() / std::f64::consts::PI) / 2.0 * n).floor()
    };

    (xtile as u32, ytile as u32)
}

pub fn center_to_base(center_x: u32, center_y: u32, radius: i32) -> (u32, u32, u32) {
    let clamped_radius = radius.max(0) as u32;
    let grid = clamped_radius * 2 + 1;
    let base_x = center_x.saturating_sub(clamped_radius);
    let base_y = center_y.saturating_sub(clamped_radius);
    (base_x, base_y, grid)
}

pub fn scale_tile_to_lower_zoom(x: u32, y: u32, from_zoom: u32, to_zoom: u32) -> (u32, u32) {
    if to_zoom >= from_zoom {
        return (x, y);
    }
    let shift = from_zoom - to_zoom;
    (x >> shift, y >> shift)
}

fn load_airport_by_icao(conn: &Connection, icao_raw: &str) -> Result<Airport> {
    let icao = icao_raw.trim().to_uppercase();
    let sql = r#"
        SELECT
          ICAO,
          FriendlyName,
          Y AS lat,
          X AS lon
        FROM Airports
        WHERE ICAO = ?
        LIMIT 1
    "#;

    let mut stmt = conn.prepare(sql)?;
    let opt_airport: Option<Airport> = stmt
        .query_map([icao.as_str()], |row: &Row| {
            let icao: String = row.get("ICAO")?;
            let name: String = row.get("FriendlyName")?;
            let lat: f64 = row.get("lat")?;
            let lon: f64 = row.get("lon")?;
            Ok(Airport {
                icao,
                name,
                lat,
                lon,
            })
        })?
        .next()
        .transpose()?;

    opt_airport.ok_or_else(|| anyhow!("ICAO {icao} not found in Airports table"))
}

pub fn build_cones_for_airport(
    airport: &Airport,
    profiles: &[ConeProfile],
    three_by_three_radius: i32,
    five_by_five_radius: i32,
) -> Vec<ConeJob> {
    let mut jobs = Vec::new();

    for profile in profiles {
        let (center_x, center_y) = latlon_to_tile(airport.lat, airport.lon, profile.base_z);
        let radius = match profile.grid {
            3 => three_by_three_radius,
            5 => five_by_five_radius,
            _ => continue,
        };

        let (base_x, base_y, grid) = center_to_base(center_x, center_y, radius);
        for out_z in &profile.out_zooms {
            jobs.push(ConeJob {
                icao: airport.icao.clone(),
                name: airport.name.clone(),
                base_z: profile.base_z,
                base_x,
                base_y,
                grid,
                out_z: *out_z,
            });
        }
    }

    jobs
}

pub fn build_cone_to_heaven_jobs_for_airport(airport: &Airport) -> Vec<ConeJob> {
    let (center18_x, center18_y) = latlon_to_tile(airport.lat, airport.lon, 18);
    let (center13_x, center13_y) = scale_tile_to_lower_zoom(center18_x, center18_y, 18, 13);
    let (center11_x, center11_y) = scale_tile_to_lower_zoom(center18_x, center18_y, 18, 11);
    let (center9_x, center9_y) = scale_tile_to_lower_zoom(center18_x, center18_y, 18, 9);

    let mut jobs = Vec::new();

    // Z13/Z14/Z15 are based on Mercator Z9, 3x3.
    let (z9_base_x, z9_base_y, z9_grid) = center_to_base(center9_x, center9_y, 1);
    for out_z in [13, 14, 15] {
        jobs.push(ConeJob {
            icao: airport.icao.clone(),
            name: airport.name.clone(),
            base_z: 9,
            base_x: z9_base_x,
            base_y: z9_base_y,
            grid: z9_grid,
            out_z,
        });
    }

    // Z16 is based on Mercator Z11, 3x3.
    let (z11_base_x, z11_base_y, z11_grid) = center_to_base(center11_x, center11_y, 1);
    jobs.push(ConeJob {
        icao: airport.icao.clone(),
        name: airport.name.clone(),
        base_z: 11,
        base_x: z11_base_x,
        base_y: z11_base_y,
        grid: z11_grid,
        out_z: 16,
    });

    // Z17 is based on Mercator Z13, 5x5.
    let (z13_base5_x, z13_base5_y, z13_grid5) = center_to_base(center13_x, center13_y, 2);
    jobs.push(ConeJob {
        icao: airport.icao.clone(),
        name: airport.name.clone(),
        base_z: 13,
        base_x: z13_base5_x,
        base_y: z13_base5_y,
        grid: z13_grid5,
        out_z: 17,
    });

    // Z18 is the inner 3x3 inside the same Mercator Z13 center.
    let (z13_base3_x, z13_base3_y, z13_grid3) = center_to_base(center13_x, center13_y, 1);
    jobs.push(ConeJob {
        icao: airport.icao.clone(),
        name: airport.name.clone(),
        base_z: 13,
        base_x: z13_base3_x,
        base_y: z13_base3_y,
        grid: z13_grid3,
        out_z: 18,
    });

    jobs
}

pub fn build_airport_cone_to_heaven_report(
    db_path: &Path,
    icaos: &[String],
) -> Result<AirportIndexReport> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open SQLite DB: {}", db_path.display()))?;

    let mut jobs = Vec::new();
    let mut airports = Vec::new();
    let mut found = Vec::new();
    let mut missing = Vec::new();

    for code in icaos {
        match load_airport_by_icao(&conn, code) {
            Ok(airport) => {
                found.push(airport.icao.clone());
                jobs.extend(build_cone_to_heaven_jobs_for_airport(&airport));
                airports.push(airport);
            }
            Err(_) => missing.push(code.trim().to_uppercase()),
        }
    }

    Ok(AirportIndexReport {
        airports_requested: icaos.iter().map(|x| x.trim().to_uppercase()).collect(),
        airports_found: found,
        airports_missing: missing,
        airports,
        jobs,
    })
}

pub fn build_airport_index_report(
    db_path: &Path,
    icaos: &[String],
    profiles: &[ConeProfile],
    three_by_three_radius: i32,
    five_by_five_radius: i32,
) -> Result<AirportIndexReport> {
    let conn = Connection::open(db_path)
        .with_context(|| format!("Failed to open SQLite DB: {}", db_path.display()))?;

    let mut jobs = Vec::new();
    let mut airports = Vec::new();
    let mut found = Vec::new();
    let mut missing = Vec::new();

    for code in icaos {
        match load_airport_by_icao(&conn, code) {
            Ok(airport) => {
                found.push(airport.icao.clone());
                jobs.extend(build_cones_for_airport(
                    &airport,
                    profiles,
                    three_by_three_radius,
                    five_by_five_radius,
                ));
                airports.push(airport);
            }
            Err(_) => missing.push(code.trim().to_uppercase()),
        }
    }

    Ok(AirportIndexReport {
        airports_requested: icaos.iter().map(|x| x.trim().to_uppercase()).collect(),
        airports_found: found,
        airports_missing: missing,
        airports,
        jobs,
    })
}

pub fn expand_job_to_tiles(job: &ConeJob) -> Vec<TileAddress> {
    if job.out_z < job.base_z {
        return Vec::new();
    }
    let factor = 1u32 << (job.out_z - job.base_z);
    let mut items = Vec::new();

    for gy in 0..job.grid {
        for gx in 0..job.grid {
            let start_x = (job.base_x + gx) * factor;
            let start_y = (job.base_y + gy) * factor;
            for dy in 0..factor {
                for dx in 0..factor {
                    items.push(TileAddress {
                        icao: job.icao.clone(),
                        z: job.out_z,
                        x: start_x + dx,
                        y: start_y + dy,
                    });
                }
            }
        }
    }

    items
}

pub fn build_download_manifest(jobs: &[ConeJob], url_template: &str) -> DownloadManifest {
    let mut seen = BTreeSet::new();
    let mut items = Vec::new();
    let mut zoom_counts: BTreeMap<u32, usize> = BTreeMap::new();
    let mut airports: BTreeSet<String> = BTreeSet::new();

    for job in jobs {
        airports.insert(job.icao.clone());
        for tile in expand_job_to_tiles(job) {
            if !seen.insert((tile.icao.clone(), tile.z, tile.x, tile.y)) {
                continue;
            }
            *zoom_counts.entry(tile.z).or_default() += 1;
            items.push(DownloadManifestItem {
                relative_path: format!("{}/{}/{}/{}.png", tile.icao, tile.z, tile.x, tile.y),
                url: render_tile_url(url_template, tile.z, tile.x, tile.y),
                icao: tile.icao,
                z: tile.z,
                x: tile.x,
                y: tile.y,
            });
        }
    }

    DownloadManifest {
        airport_count: airports.len(),
        tile_count: items.len(),
        zoom_counts,
        items,
    }
}

pub fn render_tile_url(template: &str, z: u32, x: u32, y: u32) -> String {
    template
        .replace("{z}", &z.to_string())
        .replace("{x}", &x.to_string())
        .replace("{y}", &y.to_string())
}

pub fn render_cone_spec_text(airport: &Airport, jobs: &[ConeJob]) -> String {
    let mut lines = Vec::new();
    lines.push("========================".to_string());
    lines.push(airport.icao.clone());
    lines.push("========================".to_string());
    lines.push(format!(
        "NAME: {} | LAT: {:.6} | LON: {:.6}",
        airport.name, airport.lat, airport.lon
    ));
    lines.push(String::new());

    let mut ordered_groups = vec![
        (9u32, 3u32, "ZOOM 9 -> Z13/Z14/Z15"),
        (11u32, 3u32, "ZOOM 11 -> Z16"),
        (13u32, 5u32, "ZOOM 13 -> Z17"),
        (13u32, 3u32, "ZOOM 13 -> Z18"),
    ];

    for (base_z, grid, title) in ordered_groups.drain(..) {
        let mut group_jobs = jobs
            .iter()
            .filter(|j| j.base_z == base_z && j.grid == grid)
            .collect::<Vec<_>>();
        if group_jobs.is_empty() {
            continue;
        }
        group_jobs.sort_by_key(|j| j.out_z);
        let anchor_x = group_jobs[0].base_x;
        let anchor_y = group_jobs[0].base_y;
        let center_x = anchor_x + (grid / 2);
        let center_y = anchor_y + (grid / 2);
        let out_zooms = group_jobs
            .iter()
            .map(|j| format!("Z{}", j.out_z))
            .collect::<Vec<_>>()
            .join(", ");

        lines.push(format!("[{}] ({})", title, out_zooms));
        for row in 0..grid {
            let mut cells = Vec::new();
            for col in 0..grid {
                cells.push(format!("({},{})", anchor_x + col, anchor_y + row));
            }
            lines.push(cells.join(" "));
        }
        lines.push(format!("CENTER: ({},{})", center_x, center_y));

        for job in group_jobs {
            let factor = 1u32 << (job.out_z - job.base_z);
            let tiles_side = job.grid * factor;
            let min_x = job.base_x * factor;
            let min_y = job.base_y * factor;
            let max_x = min_x + tiles_side - 1;
            let max_y = min_y + tiles_side - 1;
            lines.push(format!(
                "DOWNLOAD Z{}: X[{}..{}] Y[{}..{}] TILES={}",
                job.out_z,
                min_x,
                max_x,
                min_y,
                max_y,
                tiles_side * tiles_side
            ));
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepTimingRecord {
    pub timestamp: String,
    pub run_id: String,
    pub airports: Vec<String>,
    pub step_number: u32,
    pub step_id: String,
    pub status: String,
    pub elapsed_ms: u128,
    pub elapsed_seconds: f64,
    pub input_count: Option<usize>,
    pub output_count: Option<usize>,
    pub error_count: Option<usize>,
    pub note: String,
    pub local_text_report: String,
    pub local_csv_report: String,
    pub mirrored_text_report: Option<String>,
    pub mirrored_csv_report: Option<String>,
}

fn csv_escape_runtime(value: &str) -> String {
    if value.contains(',') || value.contains('"') || value.contains('\n') {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

fn append_step_timing_csv(path: &Path, record: &StepTimingRecord) -> Result<()> {
    let file_exists = path.exists();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("Failed to open {}", path.display()))?;

    if !file_exists {
        writeln!(
            file,
            "timestamp,run_id,airports,step_number,step_id,status,elapsed_ms,elapsed_seconds,input_count,output_count,error_count,note"
        )?;
    }

    writeln!(
        file,
        "{},{},{},{},{},{},{},{:.3},{},{},{},{}",
        csv_escape_runtime(&record.timestamp),
        csv_escape_runtime(&record.run_id),
        csv_escape_runtime(&record.airports.join("|")),
        record.step_number,
        csv_escape_runtime(&record.step_id),
        csv_escape_runtime(&record.status),
        record.elapsed_ms,
        record.elapsed_seconds,
        record
            .input_count
            .map(|value| value.to_string())
            .unwrap_or_default(),
        record
            .output_count
            .map(|value| value.to_string())
            .unwrap_or_default(),
        record
            .error_count
            .map(|value| value.to_string())
            .unwrap_or_default(),
        csv_escape_runtime(&record.note),
    )?;

    Ok(())
}

fn parse_airports_from_env() -> Vec<String> {
    std::env::var("BEAVERY_TIMING_AIRPORTS")
        .ok()
        .map(|value| {
            value
                .split(['|', ','])
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(|value| value.to_uppercase())
                .collect::<Vec<_>>()
        })
        .unwrap_or_default()
}

pub fn default_beavery_data_root() -> PathBuf {
    if let Some(path) = std::env::var_os("BEAVERY_DATA_ROOT") {
        return PathBuf::from(path);
    }

    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home)
            .join("Documents")
            .join("BEAVERY_APP")
            .join("DATA");
    }

    PathBuf::from("Documents").join("BEAVERY_APP").join("DATA")
}

pub struct StepTimer {
    step_number: u32,
    step_id: String,
    started_at: Instant,
    local_root: PathBuf,
    documents_root: PathBuf,
    run_id: String,
    airports: Vec<String>,
    finalized: Cell<bool>,
}

impl StepTimer {
    pub fn new(
        step_number: u32,
        step_id: impl Into<String>,
        local_root: impl Into<PathBuf>,
    ) -> Self {
        let step_id = step_id.into();
        let run_id = std::env::var("BEAVERY_TIMING_RUN_ID")
            .ok()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| format!("standalone-{}", Utc::now().format("%Y%m%dT%H%M%SZ")));

        Self {
            step_number,
            step_id,
            started_at: Instant::now(),
            local_root: local_root.into(),
            documents_root: default_beavery_data_root(),
            run_id,
            airports: parse_airports_from_env(),
            finalized: Cell::new(false),
        }
    }

    pub fn elapsed_seconds(&self) -> f64 {
        self.started_at.elapsed().as_secs_f64()
    }

    pub fn finish(
        &self,
        input_count: Option<usize>,
        output_count: Option<usize>,
        error_count: Option<usize>,
        note: impl Into<String>,
    ) -> Result<StepTimingRecord> {
        self.persist(
            "completed",
            input_count,
            output_count,
            error_count,
            note.into(),
        )
    }

    pub fn fail(
        &self,
        input_count: Option<usize>,
        output_count: Option<usize>,
        error_count: Option<usize>,
        note: impl Into<String>,
    ) -> Result<StepTimingRecord> {
        self.persist(
            "failed",
            input_count,
            output_count,
            error_count,
            note.into(),
        )
    }

    fn persist(
        &self,
        status: &str,
        input_count: Option<usize>,
        output_count: Option<usize>,
        error_count: Option<usize>,
        note: String,
    ) -> Result<StepTimingRecord> {
        let elapsed = self.started_at.elapsed();
        let elapsed_ms = elapsed.as_millis();
        let elapsed_seconds = elapsed.as_secs_f64();
        let local_timing_dir = self.local_root.join("beavery_timing");
        let local_text = local_timing_dir.join(format!(
            "step_{:02}_{}_timing.txt",
            self.step_number, self.step_id
        ));
        let local_csv = local_timing_dir.join("step_timings.csv");

        fs::create_dir_all(&local_timing_dir)?;

        let run_dir = self.documents_root.join("runs").join(&self.run_id);
        let mirrored_timing_dir = run_dir.join("beavery_timing");
        fs::create_dir_all(&mirrored_timing_dir)?;
        let mirrored_text = mirrored_timing_dir.join(format!(
            "step_{:02}_{}_timing.txt",
            self.step_number, self.step_id
        ));
        let mirrored_csv = mirrored_timing_dir.join("step_timings.csv");

        let text = format!(
            "timestamp={}\nrun_id={}\nairports={}\nstep_number={}\nstep_id={}\nstatus={}\nelapsed_ms={}\nelapsed_seconds={:.3}\ninput_count={}\noutput_count={}\nerror_count={}\nnote={}\nlocal_text_report={}\nlocal_csv_report={}\n",
            Utc::now().to_rfc3339(),
            self.run_id,
            self.airports.join(","),
            self.step_number,
            self.step_id,
            status,
            elapsed_ms,
            elapsed_seconds,
            input_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            output_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            error_count
                .map(|value| value.to_string())
                .unwrap_or_default(),
            note,
            local_text.display(),
            local_csv.display(),
        );

        fs::write(&local_text, &text)?;
        fs::write(&mirrored_text, &text)?;

        let record = StepTimingRecord {
            timestamp: Utc::now().to_rfc3339(),
            run_id: self.run_id.clone(),
            airports: self.airports.clone(),
            step_number: self.step_number,
            step_id: self.step_id.clone(),
            status: status.to_string(),
            elapsed_ms,
            elapsed_seconds,
            input_count,
            output_count,
            error_count,
            note: note.clone(),
            local_text_report: local_text.display().to_string(),
            local_csv_report: local_csv.display().to_string(),
            mirrored_text_report: Some(mirrored_text.display().to_string()),
            mirrored_csv_report: Some(mirrored_csv.display().to_string()),
        };

        append_step_timing_csv(&local_csv, &record)?;
        append_step_timing_csv(&mirrored_csv, &record)?;
        self.finalized.set(true);

        println!(
            "[TIMER] step={} status={} elapsed={:.3}s text_report={} mirror_root={}",
            self.step_id,
            status,
            elapsed_seconds,
            local_text.display(),
            self.documents_root.display()
        );

        Ok(record)
    }
}

impl Drop for StepTimer {
    fn drop(&mut self) {
        if !self.finalized.get() {
            let _ = self.persist(
                "aborted",
                None,
                None,
                None,
                "Step exited before writing an explicit timer result".to_string(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn latlon_to_tile_matches_known_jfk_anchor() {
        let (x, y) = latlon_to_tile(40.63993, -73.77869, 9);
        assert_eq!((x, y), (151, 192));
        let (base_x, base_y, grid) = center_to_base(x, y, 1);
        assert_eq!((base_x, base_y, grid), (150, 191, 3));
    }

    #[test]
    fn cone_expansion_has_expected_count() {
        let job = ConeJob {
            icao: "KJFK".to_string(),
            name: "John F Kennedy Intl".to_string(),
            base_z: 13,
            base_x: 2416,
            base_y: 3081,
            grid: 3,
            out_z: 18,
        };
        let tiles = expand_job_to_tiles(&job);
        assert_eq!(tiles.len(), 9 * 1024);
    }

    #[test]
    fn z18_backsolve_matches_known_jfk_layout() {
        let airport = Airport {
            icao: "KJFK".to_string(),
            name: "John F Kennedy Intl".to_string(),
            lat: 40.63993,
            lon: -73.77869,
        };
        let jobs = build_cone_to_heaven_jobs_for_airport(&airport);
        let z9 = jobs
            .iter()
            .find(|j| j.base_z == 9 && j.out_z == 13)
            .expect("z9/z13 job missing");
        let z11 = jobs
            .iter()
            .find(|j| j.base_z == 11 && j.out_z == 16)
            .expect("z11/z16 job missing");
        let z17 = jobs
            .iter()
            .find(|j| j.base_z == 13 && j.out_z == 17)
            .expect("z13/z17 job missing");
        let z18 = jobs
            .iter()
            .find(|j| j.base_z == 13 && j.out_z == 18)
            .expect("z13/z18 job missing");

        assert_eq!((z9.base_x, z9.base_y, z9.grid), (150, 191, 3));
        assert_eq!((z11.base_x, z11.base_y, z11.grid), (603, 769, 3));
        assert_eq!((z17.base_x, z17.base_y, z17.grid), (2415, 3080, 5));
        assert_eq!((z18.base_x, z18.base_y, z18.grid), (2416, 3081, 3));
    }

    #[test]
    fn z18_backsolve_matches_known_ksan_layout() {
        let airport = Airport {
            icao: "KSAN".to_string(),
            name: "San Diego Intl".to_string(),
            lat: 32.73356,
            lon: -117.1897,
        };
        let jobs = build_cone_to_heaven_jobs_for_airport(&airport);
        let z9 = jobs
            .iter()
            .find(|j| j.base_z == 9 && j.out_z == 13)
            .expect("z9/z13 job missing");
        let z11 = jobs
            .iter()
            .find(|j| j.base_z == 11 && j.out_z == 16)
            .expect("z11/z16 job missing");
        let z17 = jobs
            .iter()
            .find(|j| j.base_z == 13 && j.out_z == 17)
            .expect("z13/z17 job missing");
        let z18 = jobs
            .iter()
            .find(|j| j.base_z == 13 && j.out_z == 18)
            .expect("z13/z18 job missing");

        assert_eq!((z9.base_x, z9.base_y, z9.grid), (88, 205, 3));
        assert_eq!((z11.base_x, z11.base_y, z11.grid), (356, 825, 3));
        assert_eq!((z17.base_x, z17.base_y, z17.grid), (1427, 3304, 5));
        assert_eq!((z18.base_x, z18.base_y, z18.grid), (1428, 3305, 3));
    }
}
