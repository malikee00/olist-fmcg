from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml


class ConfigError(RuntimeError):
    pass


def _read_yaml(path: str | Path) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise ConfigError(f"Config file not found: {p}")
    with p.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigError(f"Invalid YAML root (expected dict): {p}")
    return data


def _require(d: Dict[str, Any], key_path: str) -> Any:
    cur: Any = d
    for k in key_path.split("."):
        if not isinstance(cur, dict) or k not in cur:
            raise ConfigError(f"Missing required config key: {key_path}")
        cur = cur[k]
    return cur


@dataclass(frozen=True)
class Settings:
    raw_incoming_dir: Path
    raw_archive_dir: Path
    checkpoint_dir: Path
    bronze_dir: Path


@dataclass(frozen=True)
class BronzeTableSpec:
    name: str
    file: str
    timestamp_cols: List[str]
    numeric_cols: List[str]


@dataclass(frozen=True)
class BronzeTablesConfig:
    source_dir: Path
    bronze_dir: Path
    file_format: str
    write_format: str
    partition_by: str
    write_mode: str
    tables: List[BronzeTableSpec]


def load_settings(path: str | Path) -> Settings:
    cfg = _read_yaml(path)

    raw_incoming_dir = Path(_require(cfg, "paths.raw_incoming_dir"))
    raw_archive_dir = Path(_require(cfg, "paths.raw_archive_dir"))
    checkpoint_dir = Path(_require(cfg, "paths.checkpoint_dir"))
    bronze_dir = Path(_require(cfg, "paths.bronze_dir"))

    return Settings(
        raw_incoming_dir=raw_incoming_dir,
        raw_archive_dir=raw_archive_dir,
        checkpoint_dir=checkpoint_dir,
        bronze_dir=bronze_dir,
    )


def load_bronze_tables(path: str | Path) -> BronzeTablesConfig:
    cfg = _read_yaml(path)

    source_dir = Path(_require(cfg, "source.source_dir"))
    bronze_dir = Path(_require(cfg, "source.bronze_dir"))
    file_format = str(_require(cfg, "source.file_format")).lower()
    write_format = str(_require(cfg, "source.write_format")).lower()
    partition_by = str(_require(cfg, "source.partition_by"))
    write_mode = str(_require(cfg, "source.write_mode")).lower()

    tables_raw = _require(cfg, "tables")
    if not isinstance(tables_raw, list) or not tables_raw:
        raise ConfigError("Invalid tables list (expected non-empty list).")

    tables: List[BronzeTableSpec] = []
    for t in tables_raw:
        if not isinstance(t, dict):
            raise ConfigError("Each table spec must be a dict.")
        name = t.get("name")
        file = t.get("file")
        if not name or not file:
            raise ConfigError("Each table spec must include 'name' and 'file'.")

        ts_cols = t.get("timestamp_cols") or []
        num_cols = t.get("numeric_cols") or []

        if not isinstance(ts_cols, list) or not isinstance(num_cols, list):
            raise ConfigError(f"Invalid column lists for table={name}")

        tables.append(
            BronzeTableSpec(
                name=str(name),
                file=str(file),
                timestamp_cols=[str(x) for x in ts_cols],
                numeric_cols=[str(x) for x in num_cols],
            )
        )

    return BronzeTablesConfig(
        source_dir=source_dir,
        bronze_dir=bronze_dir,
        file_format=file_format,
        write_format=write_format,
        partition_by=partition_by,
        write_mode=write_mode,
        tables=tables,
    )
