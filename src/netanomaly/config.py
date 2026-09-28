"""Pipeline configuration, loaded from config.yaml.

Defaults assume a small memory budget on a CPU-only machine: DuckDB spills to
disk instead of growing, and Python only ever holds one bounded batch.
"""

from __future__ import annotations

from pathlib import Path

import yaml
from pydantic import BaseModel, Field, field_validator


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


class DQSettings(BaseModel):
    # Daily volume is compared with the median of the previous `trailing_days` calendar days (strictly earlier).
    trailing_days: int = Field(default=7, ge=1)
    min_history_days: int = Field(default=3, ge=1)
    volume_ratio_low: float = Field(default=0.5, gt=0)
    volume_ratio_high: float = Field(default=2.0, gt=0)


class BaselineSettings(BaseModel):
    # Host baselines (V2-2, baselines.py): rows on day D use only the `lookback_days` whole UTC days before D.
    lookback_days: int = Field(default=7, ge=1)
    # A level (host, peer, global) qualifies with this much history and MAD > 0; otherwise fall back.
    min_windows: int = Field(default=30, ge=2)
    min_days: int = Field(default=2, ge=1)
    min_peer_hosts: int = Field(default=5, ge=2)  # peer and global levels only


class TimingSettings(BaseModel):
    # Timing regularity (V2-4, timing.py): a window starting at W uses flows in [W - history_hours, W) only.
    # At most 24 h, so a window's history spans its own day and the day before.
    history_hours: int = Field(default=2, ge=1, le=24)
    # A (src_ip, dst_ip) series needs this many distinct flow_start instants in the history (min_events - 1 gaps).
    min_events: int = Field(default=10, ge=3)


class FeatureCardSettings(BaseModel):
    # Feature cards (V2-5, feature_cards.py). PSI reference period = lake days [warmup_days, warmup_days +
    # reference_days), by position in the lake's sorted UTC days; every day is compared with it.
    # warmup_days skips the days where prior-history features cannot be complete (baselines need 2 earlier days).
    warmup_days: int = Field(default=2, ge=0)
    reference_days: int = Field(default=1, ge=1)
    psi_bins: int = Field(default=10, ge=2)  # quantile bins of the reference values, plus one NULL bin
    redundancy_threshold: float = Field(default=0.9, gt=0, le=1)  # |Spearman rho| at or above: flagged redundant


class SplitSettings(BaseModel):
    # Time-based split (V3, iforest.time_split): the first floor(n_days * train_fraction) UTC days of the feature
    # table train the model; only later days are scored. Positional, so it never looks at labels.
    train_fraction: float = Field(default=0.5, gt=0, lt=1)


class StabilitySettings(BaseModel):
    # Stability report (V3, stability.py): label-free agreement of rankings on the held-out score days.
    seeds: int = Field(default=10, ge=2)  # seed-stability models; their mean score is the reference
    sample_sizes: list[int] = Field(default=[500, 1_000, 2_000, 5_000, 10_000, 20_000, 50_000], min_length=1)
    curve_seeds: int = Field(default=5, ge=1)  # models per sample size (seeds disjoint from the reference)

    @field_validator("sample_sizes")
    @classmethod
    def _positive(cls, v: list[int]) -> list[int]:
        if min(v) < 1:
            raise ValueError("sample sizes must be >= 1")
        return v


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
    dq: DQSettings = DQSettings()
    baseline: BaselineSettings = BaselineSettings()
    timing: TimingSettings = TimingSettings()
    feature_cards: FeatureCardSettings = FeatureCardSettings()
    split: SplitSettings = SplitSettings()
    model: ModelSettings = ModelSettings()
    stability: StabilitySettings = StabilitySettings()
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
