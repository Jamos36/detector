"""Pipeline configuration, loaded from config.yaml.

Defaults assume a small memory budget on a CPU-only machine: DuckDB spills to
disk instead of growing, and Python only ever holds one bounded batch.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field


class DuckDBSettings(BaseModel):
    memory_limit: str = "2GB"
    threads: int = Field(default=4, ge=1)
    temp_directory: Path = Path("data/tmp/duckdb")


class Paths(BaseModel):
    raw: Path = Path("data/raw")
    lake: Path = Path("data/lake")
    features: Path = Path("data/features")
    models: Path = Path("data/models")
    outputs: Path = Path("data/outputs")
    quarantine: Path = Path("data/quarantine")


class IngestSettings(BaseModel):
    # Above this share of rejected rows the whole file is quarantined: a file problem, not bad records.
    max_reject_fraction: float = Field(default=0.05, ge=0, le=1)


class ModelSettings(BaseModel):
    train_sample_rows: int = Field(default=200_000, ge=1_000)
    n_estimators: int = Field(default=200, ge=10)
    max_samples: int = Field(default=256, ge=16)
    seed: int = 42
    n_jobs: int = Field(default=2, ge=1)


class Settings(BaseModel):
    paths: Paths = Paths()
    duckdb: DuckDBSettings = DuckDBSettings()
    ingest: IngestSettings = IngestSettings()
    model: ModelSettings = ModelSettings()
    batch_rows: int = Field(default=50_000, ge=1_000)
    window_minutes: int = Field(default=5, ge=1)
    alert_budget_per_day: int = Field(default=100, ge=1)

    def resolve(self, root: Path) -> Settings:
        """Return a copy with every relative path anchored at `root`."""

        def anchor(p: Path) -> Path:
            return p if p.is_absolute() else root / p

        paths = Paths(**{k: anchor(v) for k, v in self.paths.model_dump().items()})
        duck = self.duckdb.model_copy(update={"temp_directory": anchor(self.duckdb.temp_directory)})
        return self.model_copy(update={"paths": paths, "duckdb": duck})


def load_settings(path: Path | None = None) -> Settings:
    """Load settings from YAML; missing file or keys fall back to defaults."""
    if path is None or not path.exists():
        return Settings()
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    return Settings.model_validate(data).resolve(path.parent.resolve())
