# Step 9 Move SQLs

`move_sqls_rust` is the Rust-first output handoff stage for the pipeline.

## What It Does

- Moves or copies final SQLite-style artifacts out of the imagery workspace.
- Accepts either a local path or a mounted network-share path as the destination.
- Saves the destination you provide so later step-9 runs keep using it until you change or clear it.
- Writes a JSON move report so the pipeline runner can verify the handoff even when the destination is outside the run folder.

## Default Destination Behavior

Step 9 now resolves the destination in this order:

1. `--set-default-destination <PATH>`
2. `--output-dir <PATH>`
3. Saved config at `~/.beavery/step9_destination.json`
4. Fallback to `<run>/final`

When you pass `--output-dir`, that path is automatically saved as the new default unless you clear it later.

## Common Commands

Set a new default destination and copy artifacts there:

```bash
cargo run -p move_sqls_rust -- \
  --input-dir /path/to/run/imagery \
  --output-dir /Volumes/BEAVERY_SHARE/SQLITE_OUTPUT \
  --copy
```

Show the current saved destination:

```bash
cargo run -p move_sqls_rust -- \
  --input-dir /path/to/run/imagery \
  --show-default-destination \
  --dry-run
```

Clear the saved destination:

```bash
cargo run -p move_sqls_rust -- \
  --input-dir /path/to/run/imagery \
  --clear-default-destination \
  --dry-run
```

## Pipeline Behavior

The main pipeline now calls step 9 without forcing `--output-dir`.

- If you have already saved a destination, step 9 uses it.
- If not, step 9 falls back to the run-local `final` folder.
- The runner stores the verification report at `<run>/state/step9_move_sqls_report.json`.
