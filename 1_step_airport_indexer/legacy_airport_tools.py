#!/usr/bin/env python3
"""
Deprecated compatibility wrapper for legacy step 1 airport helper scripts.

This file replaces:
  - build_all_airports_from_minimal_specs.py
  - build_cones_from_tile_index.py
  - spec_generator.py
  - spec_generator2.py
  - spec_generator3.py
  - spec_manual.py

Default behavior is Rust-first. Use the same subcommands as the Rust binary:
  python3 crates/1_step_airport_indexer/legacy_airport_tools.py generate-specs ...
  python3 crates/1_step_airport_indexer/legacy_airport_tools.py build-geotiffs ...
  python3 crates/1_step_airport_indexer/legacy_airport_tools.py build-from-index ...
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def main(argv: list[str]) -> int:
    workspace_root = Path(__file__).resolve().parents[2]
    rust_cmd = [
        "cargo",
        "run",
        "-p",
        "step_airport_indexer",
        "--bin",
        "airport_index_toolkit",
        "--",
        *argv,
    ]

    print(
        "[legacy_airport_tools] deprecated Python wrapper invoked; forwarding to Rust tool:",
        " ".join(rust_cmd),
        file=sys.stderr,
    )
    completed = subprocess.run(rust_cmd, cwd=workspace_root)
    return completed.returncode


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
