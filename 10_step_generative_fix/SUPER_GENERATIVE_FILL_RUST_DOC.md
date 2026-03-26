# Step 10 Super Generative Fill

Step 10 is now a Rust-first repair stage with a Rust maintenance binary, shared Rust script tools, and one retained Python maintenance fallback:

- `src/main.rs`
  - CLI entrypoint for the Rust repair planner/writer.
- `src/lib.rs`
  - Target discovery, bad-tile detection, candidate synthesis, scoring, and worldfile writing.
- `src/bin/step10_legacy_tools_rust.rs`
  - Rust default for the old maintenance chores that used to live in many one-off Python scripts.
- `../X_script_tools_rust`
  - Shared Rust implementations for patch catalog generation, library matching, pattern generation, and tile scanning.
- `step10_legacy_tools.py`
  - Python fallback for the same maintenance jobs if the Rust utility hits an edge case.

The old airport-specific Python one-offs have been removed from this folder. Their useful ideas were either folded into Rust or generalized into the Rust utility and the one retained Python fallback script.

## What Step 10 Does

Step 10 repairs missing or poisoned tiles after the earlier imagery pipeline has already produced a tile set. It is deterministic and geometry-aware, not a diffusion model or freehand AI painter.

Primary recovery methods:

1. `child_downsample`
2. `parent_upsample`
3. `adjacent_zoom_blend`
4. `multi_zoom_synthesis_zN`
5. `neighbor_clone`
6. `neighbor_blend`
7. `large_gap_idw`
8. `patch_library`
9. `solid_context_fill`

If `--apply` is not used, Step 10 stays in planning mode and only writes a repair plan JSON.

`parent_upsample` and the lower-zoom branch inside `multi_zoom_synthesis_zN` now use a shared Rust upsample helper that enlarges the full parent tile to `512x512` first and then splits out the child quadrants. That preserves more surrounding context at quadrant edges than the older direct `128 -> 256` crop-resize path.

## Function Summary

### Rust Public Functions

- `discover_xyz_tiles(root)`
  - Builds a zoom-to-coordinate inventory from namespaced tile folders.
- `discover_tile_inventory(root)`
  - Builds the namespace-aware tile index used by the planner.
- `build_super_repair_plan(root, zooms, airports, max_missing, apply, config)`
  - Main Step 10 function. Finds targets, tests methods, scores candidates, and optionally writes repaired tiles.
- `write_worldfiles_png(path, zoom, x, y)`
  - Writes `.pgw` sidecars for generated PNG tiles.

### Rust Planner Responsibilities

- Target discovery
  - Flags missing tiles and optionally existing bad tiles.
- Tile health checks
  - Uses file size, placeholder hashes, dark/flat signatures, and decode success.
- Namespace safety
  - Keeps airport folders isolated so identical `z/x/y` coordinates do not collide across airports.
- Candidate scoring
  - Uses texture, dynamic range, method bonuses, and penalties so geometric sources beat weak guesses.
- Context tuning
  - Detects `water`, `greenery`, or `neutral` neighborhood context and adjusts color accordingly.
- Seam blending
  - Optionally feathers borders against real neighbor tiles to reduce visible seams.

### Rust Maintenance Utility

`step10_legacy_tools_rust` is now the default maintenance utility:

- `download-window`
  - Download one or more rectangular tile windows from a URL template.
- `flatten-xy`
  - Convert flat `x_y.png` tiles into nested `x/y.png` layout.
- `paint-window`
  - Stamp one source image across a flat tile window and optionally write worldfiles.
- `rebuild-cones`
  - Rebuild target GeoTIFF cones from nearby zoom levels using GDAL.

Example:

```bash
cargo run -p generative_fix_rust --bin step10_legacy_tools_rust -- \
  download-window \
  --url-template "https://example/{path}" \
  --dest-root /path/to/tiles \
  --window KJFK:16:9664-9727:12352-12415
```

### Python Fallback Utility

`step10_legacy_tools.py` keeps only the generic support jobs that are still worth keeping around:

- `download-window`
  - Download one or more rectangular tile windows from a URL template.
- `flatten-xy`
  - Convert flat `x_y.png` tiles into nested `x/y.png` layout.
- `paint-window`
  - Stamp one source image across a flat tile window and optionally write worldfiles.
- `rebuild-cones`
  - Rebuild target GeoTIFF cones from nearby zoom levels using GDAL.

Use the Python version only when the Rust utility hits an environment-specific issue.

### Patch Matching Runtime Policy

Step 10 patch-library matching is now Rust-first inside `src/lib.rs`.

- Primary path
  - `script_tools_rust::match_library_image(...)`
- Fallback path
  - `python3 scripts/library_matcher.py --python-only ...`

That means step 10 no longer shells directly into Python as its default matcher.

## Legacy Python Merge Map

The old scripts were retired for one of two reasons:

- Their ideas were absorbed into Rust:
  - placeholder hash detection
  - dark/flat bad-tile detection
  - higher-zoom reconstruction
  - alternate zoom selection
  - adjacent zoom blending
  - neighbor fallback and neighbor blending
  - solid context fallback
- Their remaining useful behavior was generalized into `step10_legacy_tools.py`:
  - rectangular downloads
  - flat-to-nested tile reshaping
  - fill-image stamping
  - GDAL cone rebuilding

## High-Value CLI Controls

- `--apply`
  - Writes repaired PNG tiles and worldfiles instead of plan-only mode.
- `--airport`
  - Restricts work to one or more airport namespaces.
- `--zoom`
  - Restricts work to the zoom levels you actually want touched.
- `--max-missing`
  - Caps batch size so operators do not accidentally blast a huge area.
- `--skip-bad-tiles`
  - Repairs only missing tiles; leaves suspicious existing tiles alone.
- `--placeholder-hash`
  - Adds known placeholder hashes for your data source.
- `--neighbor-radius`
  - Controls same-zoom neighbor search.
- `--large-gap-radius`
  - Controls the radius for the larger IDW-style synthesis fallback.
- `--disable-context-tuning`
  - Turns off water/greenery color shaping.
- `--seam-aware-blend`
  - Tries to make repaired borders sit more cleanly against real neighbors.
- `--patch-library` and `--enable-patch-matching`
  - Enables patch matching for natural-surface fallback work.

## Recommended Operator Workflow

1. Run Step 10 in plan mode first.
2. Limit the run to one airport and one zoom when validating a new batch.
3. Review `repair_plan.json` before writing anything.
4. Only then rerun with `--apply`.
5. Visually inspect seams, runway edges, taxiway markings, shorelines, and terminal roofs after write mode.

Example planning run:

```bash
cargo run -p generative_fix_rust -- \
  --tiles-root /path/to/tiles \
  --airport KJFK \
  --zoom 16 \
  --max-missing 200 \
  --out-plan /path/to/repair_plan.json
```

Example write run:

```bash
cargo run -p generative_fix_rust -- \
  --tiles-root /path/to/tiles \
  --airport KJFK \
  --zoom 16 \
  --max-missing 200 \
  --seam-aware-blend \
  --apply \
  --out-plan /path/to/repair_plan.json
```

## Best Practices For Untrained Employees Making Airports

- Work one airport at a time.
- Work one zoom at a time until you know the output is clean.
- Use plan mode before write mode every single time.
- Do not change thresholds unless a lead has told you why.
- Do not use patch fills to invent runways, taxi lines, numbers, hold-short bars, ramps, or terminals.
- Use patch matching and paint tools only for natural surfaces such as grass, dirt, water, or generic background texture.
- If a tile touches runway paint, terminal roofs, bridges, or coastline edges, review it manually after repair.
- If `unresolved_count` is not zero, escalate instead of forcing random fixes.
- If a repair batch is larger than expected, stop and re-check your airport and zoom filters.
- Keep a copy of the tile area before running `--apply`.

## What Not To Expect

Step 10 is not a magic art generator. It is a structured repair system optimized for reliability:

- It prefers real geometry-derived sources over invented imagery.
- It uses neighbors and zoom relationships, not unrestricted hallucination.
- It is safest for gaps, placeholders, dark tiles, and natural-surface continuity.
- It is least safe for hard man-made features with strict geometry.
