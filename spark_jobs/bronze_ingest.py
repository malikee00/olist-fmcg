from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from configs.loaders import load_settings, load_bronze_tables


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--settings", required=True, help="Path to configs/settings.yaml")
    parser.add_argument("--bronze-tables", required=True, help="Path to configs/bronze_tables.yaml")
    args = parser.parse_args()

    settings = load_settings(args.settings)
    bronze_cfg = load_bronze_tables(args.bronze_tables)

    print("[OK] Config loaded.")
    print("Incoming  :", settings.raw_incoming_dir)
    print("Archive   :", settings.raw_archive_dir)
    print("Checkpoint:", settings.checkpoint_dir)
    print("Bronze    :", settings.bronze_dir)
    print("Tables    :", [t.name for t in bronze_cfg.tables])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
