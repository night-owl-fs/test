# Step 10 Generative Fix

Version `1.5.2`

## Rust-First Tools

Step 10 is the Rust-first repair stage for missing, poisoned, or low-quality tiles. It now ships with two Rust entrypoints plus shared Rust script tools:

- `generative_fix_rust`
  - Main repair planner/writer for missing or poisoned tiles.
- `step10_legacy_tools_rust`
  - Rust replacement for the old maintenance helpers such as download-window, flatten-xy, paint-window, and rebuild-cones.
- `script_tools_rust`
  - Shared Rust patch-library matcher/catalog/pattern/scanner support used by step 10 and the root `scripts/` wrappers.

Examples:

```bash
cargo run -p generative_fix_rust -- --help
cargo run -p generative_fix_rust --bin step10_legacy_tools_rust -- --help
```

## What Changed In 1.5.2

- Parent-derived tile recovery now uses a shared Rust upsample helper instead of the older direct crop-resize path.
- Lower-zoom synthesis reuses the same full-parent-to-child split flow, which preserves more surrounding context at tile edges.
- Step 10 remains deterministic and geometry-aware. It is still a repair system, not a freeform image generator.

## Recommended Use

Run plan mode first, then write mode only after reviewing the output JSON:

```bash
cargo run -p generative_fix_rust -- \
  --tiles-root /path/to/tiles \
  --airport KJFK \
  --zoom 16 \
  --max-missing 200 \
  --out-plan /path/to/repair_plan.json
```

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

## Python Fallback

- `step10_legacy_tools.py`
  - Keep this only as the fallback path if the Rust maintenance binary hits an environment-specific issue.
- `scripts/library_matcher.py`
  - Now a Rust-first wrapper around `library_matcher_rust`, with legacy Python matching kept only as a fallback.

## Detailed Notes

See [SUPER_GENERATIVE_FILL_RUST_DOC.md](/Users/raverynfs/Projects/BEAVERY/crates/10_step_generative_fix/SUPER_GENERATIVE_FILL_RUST_DOC.md).
