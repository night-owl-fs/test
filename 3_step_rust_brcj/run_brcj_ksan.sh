#!/usr/bin/env bash
set -euo pipefail

# Path to the compiled Rust binary
BIN="/Users/raverynfs/Downloads/brcj_rust/target/release/brcj_rust"

# KSAN tile roots
SRC_ROOT="/Volumes/T9/TILES/KSAN"
DST_ROOT="/Volumes/T9/TILES_BRCJ/KSAN"

mkdir -p "$DST_ROOT"

run_zoom() {
  local z="$1"

  echo ""
  echo "======================================="
  echo "🔥 RUNNING KSAN Z$z"
  echo "======================================="

  "$BIN" \
    --input  "$SRC_ROOT/$z" \
    --output "$DST_ROOT/$z" \
    --zoom "$z" \
    --workers 10

  echo "=== DONE Z$z ==="
}

# Z13: LOW RES HEAVEN
run_zoom 13

# Z14: LOW RES
run_zoom 14

# Z15–16: MID RES
run_zoom 15
run_zoom 16

# Z17–18: HIGH RES
run_zoom 17
run_zoom 18

echo ""
echo "======================================="
echo "🎉 ALL KSAN ZOOMS 13–18 COMPLETED"
echo "======================================="
