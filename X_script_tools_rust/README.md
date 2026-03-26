# X Script Tools Rust

Shared Rust crate for the root helper scripts and patch-library support code.

## Binaries

- `generate_patch_catalog_rust`
- `library_matcher_rust`
- `pattern_generator_rust`
- `tile_scanner_rust`

## Runtime Policy

- Default path: the Rust binaries in this crate
- Fallback path: the legacy Python implementations in `/scripts`

The Python files in `/scripts` now act as Rust-first wrappers. They try these Rust binaries first and only fall back to Python if the Rust command fails or `--python-only` is used.
