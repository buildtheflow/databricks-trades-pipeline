"""
config.py
Loads environment-specific config from YAML.
PIPELINE_ENV environment variable drives which config is loaded (dev/prod).
"""

import os
import yaml
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class PathConfig:
    raw: str
    bronze: str
    silver: str
    quarantine: str
    gold: str


@dataclass
class TableConfig:
    bronze: str
    silver: str
    gold: str


@dataclass
class WriteConfig:
    bronze_mode: str
    silver_mode: str
    gold_mode: str


@dataclass
class DQConfig:
    fail_on_quarantine: bool
    max_quarantine_pct: float


@dataclass
class PipelineConfig:
    env: str
    catalog: str
    paths: PathConfig
    tables: TableConfig
    write: WriteConfig
    dq: DQConfig


def load_config(env: str = None) -> PipelineConfig:
    """
    Load config for the given environment.
    Falls back to PIPELINE_ENV env var, then defaults to 'dev'.
    """
    env = env or os.getenv("PIPELINE_ENV", "dev")
    config_path = Path(__file__).parents[2] / "config" / f"{env}.yml"

    if not config_path.exists():
        raise FileNotFoundError(f"Config not found: {config_path}")

    with open(config_path) as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(
        env=raw["env"],
        catalog=raw["catalog"],
        paths=PathConfig(**raw["paths"]),
        tables=TableConfig(**raw["tables"]),
        write=WriteConfig(**raw["write"]),
        dq=DQConfig(**raw["dq"]),
    )
