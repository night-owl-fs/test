# Patch Library

This folder holds small reusable patch tiles for quick fills and pattern stamps.

Structure

- `catalog.json` - index of available patches and metadata (context class, tags, avg color, size).
- `<preset-name>_<size>.png` - patch image files.

Usage

- Use `cargo run -p script_tools_rust --bin pattern_generator_rust -- ...` for the direct Rust path.
- Use `scripts/pattern_generator.py` if you want the Rust-first wrapper with Python fallback baked in.
- Use `cargo run -p script_tools_rust --bin library_matcher_rust -- ...` for the direct Rust matcher.
- Use `scripts/library_matcher.py` if you want the Rust-first wrapper with Python fallback baked in.
- Keep library images small (e.g., 64x64 or 128x128) and reuse by scaling/compositing into tiles.

Licensing

- Generated patches are created procedurally by this project (no external license).
- If you add third-party patches, include license notes in `catalog.json` entries.
