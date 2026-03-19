# BRCJ Rust Darkener

Simple Rust implementation of the "Babe Ruth Crazy Jeff" darkening pass.

## Build

```bash
cargo build --release
```

## Run examples

High-res (Z17/18/19):

```bash
./target/release/brcj_rust   --input "/path/to/z19_tiles"   --output "/path/to/z19_brcj"   --zoom 19   --workers 12
```

Mid-res (Z15/16):

```bash
./target/release/brcj_rust   --input "/path/to/z15_tiles"   --output "/path/to/z15_brcj"   --zoom 15   --workers 8
```

Z14:

```bash
./target/release/brcj_rust   --input "/path/to/z14_tiles"   --output "/path/to/z14_brcj"   --zoom 14   --workers 8
```

Z13 Heaven:

```bash
./target/release/brcj_rust   --input "/path/to/z13_tiles"   --output "/path/to/z13_brcj"   --zoom 13   --workers 8
```

Or use `--group high|mid|z14|z13` directly instead of `--zoom`.

## Coating Ledger (CSV)

Use `--ledger-file` to append one CSV row per source PNG processed by BRCJ.

Example:

```bash
./target/release/brcj_rust \
  --input "/path/to/z16_tiles" \
  --output "/path/to/z16_brcj" \
  --zoom 16 \
  --workers 12 \
  --ledger-file "/path/to/BRCJ_COATED_TILES.csv"
```

By default, BRCJ now reprocesses files even if output already exists.  
Use `--skip-existing` to keep prior output files unchanged.
`--overwrite` is still accepted for old scripts and behaves the same as default.

CSV columns:

`run_id,group,zoom,input_root,output_root,source_png,output_png,status,note`

`status` is one of: `processed`, `skipped`, `error`.

## RUST_PGW

This repo now includes a dedicated PGW generator binary:

```bash
cargo run --release --bin rust_pgw -- \
  --root "/path/to/tiles_root" \
  --recursive \
  --write-png-pgw \
  --overwrite
```

Notes:
- If `--zoom` is omitted, `rust_pgw` uses the numeric parent folder (for example `.../16/19328_24608.png` -> zoom 16).
- It writes `.pgw` sidecars, plus `.png.pgw` when `--write-png-pgw` is enabled.
