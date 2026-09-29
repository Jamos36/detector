"""PoC experiment configuration (YAML). Paths may use environment variables (`${NETANOMALY_DATA}`), so a config can
live in the repo while the data and outputs live elsewhere. Nothing here assumes a real data location."""

from __future__ import annotations

import itertools
import os
import re
from datetime import date
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from netanomaly.config import DuckDBSettings

EpochUnit = Literal["s", "ms", "us", "ns"]
Scaler = Literal["none", "standard", "robust", "maxabs"]
BAND_ORDER = ("Critical", "High", "Medium", "Low")  # most to least severe review band; below Low = below_label


class Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class InputSettings(Strict):
    paths: list[str] = Field(min_length=1)  # Parquet files, directories or globs
    # canonical field -> source column. Unset fields default to the contract's raw name (netflow_v1.yaml).
    field_map: dict[str, str] = {}
    # canonical timestamp field -> unit, required when the source stores an integer epoch
    epoch_unit: dict[str, EpochUnit] = {}


class FeatureSelection(Strict):
    include: list[str] | None = None  # None = every defined feature whose source columns are mapped
    exclude: list[str] = []


class Period(Strict):
    start: date  # inclusive, UTC midnight
    end: date  # exclusive, UTC midnight


class SplitSettings(Strict):
    # Explicit chronological periods (recommended), or fractions of the distinct UTC days in the data.
    train: Period | None = None
    validation: Period | None = None
    test: Period | None = None
    fractions: tuple[float, float, float] = (0.6, 0.2, 0.2)
    # Drop windows inside a supplied pentest interval (or its buffer) from training. Never uses them as positives.
    exclude_annotated_from_train: bool = False

    @model_validator(mode="after")
    def _consistent(self) -> SplitSettings:
        if (self.validation or self.test) and not self.train:
            raise ValueError("an explicit split needs at least `train`")
        if abs(sum(self.fractions) - 1) > 1e-9 or min(self.fractions) < 0 or self.fractions[0] <= 0:
            raise ValueError("split fractions must be >= 0, sum to 1, with a positive training share")
        return self


class IForestSettings(Strict):
    enabled: bool = True
    n_estimators: int = Field(default=200, ge=10)
    max_samples: int | float | Literal["auto"] = 256
    max_features: float = Field(default=1.0, gt=0, le=1)
    # Only affects sklearn's binary predict(); scores and bands never use it. Not a measured anomaly rate.
    contamination: float | Literal["auto"] = "auto"
    max_train_rows: int = Field(default=200_000, ge=100)
    scaler: Scaler = "none"  # trees are scale-invariant; kept configurable for symmetry


class OCSVMSettings(Strict):
    enabled: bool = True
    kernel: Literal["rbf", "linear", "poly", "sigmoid"] = "rbf"
    nu: float = Field(default=0.05, gt=0, le=1)  # bound on training margin errors; not an attack rate
    gamma: float | Literal["scale", "auto"] = "scale"
    max_train_rows: int = Field(default=20_000, ge=100)  # the fit is roughly quadratic in rows
    hard_max_train_rows: int = Field(default=100_000, ge=100)
    scaler: Scaler = "standard"


class ModelSettings(Strict):
    seed: int = 42
    iforest: IForestSettings = IForestSettings()
    ocsvm: OCSVMSettings = OCSVMSettings()
    max_matrix_mb: int = Field(default=1024, ge=1)  # refuse a training matrix larger than this
    n_jobs: int = Field(default=2, ge=1)


class BandSettings(Strict):
    # Rank/review bands, NOT probabilities or severities. Cutoffs come from the reference period's raw scores.
    reference: Literal["validation", "train"] = "validation"
    mode: Literal["quantile", "budget"] = "quantile"
    # quantile mode: a window is in the highest band whose reference quantile its raw score reaches
    quantiles: dict[str, float] = {"Critical": 0.999, "High": 0.995, "Medium": 0.99, "Low": 0.975}
    # budget mode: expected windows per day at or above each band on the reference period (cumulative)
    budget_per_day: dict[str, float] = {"Critical": 2, "High": 10, "Medium": 25, "Low": 60}
    below_label: str = "Benign"  # means "below the configured review threshold", not "safe"

    @model_validator(mode="after")
    def _ordered(self) -> BandSettings:
        for name, values, increasing in (("quantiles", self.quantiles, False),
                                         ("budget_per_day", self.budget_per_day, True)):
            if set(values) != set(BAND_ORDER):
                raise ValueError(f"bands.{name} needs exactly {BAND_ORDER}")
            seq = [values[b] for b in BAND_ORDER]
            pairs = list(itertools.pairwise(seq))
            if not all((a < b) if increasing else (a > b) for a, b in pairs):
                raise ValueError(f"bands.{name} must be strictly ordered from Critical to Low")
        if not all(0 < q < 1 for q in self.quantiles.values()):
            raise ValueError("band quantiles must be in (0, 1)")
        if min(self.budget_per_day.values()) <= 0:
            raise ValueError("band budgets must be > 0")
        if self.below_label in BAND_ORDER:
            raise ValueError("below_label must differ from the review band names")
        return self


class DiagnosticSettings(Strict):
    top_k_per_day: int = Field(default=20, ge=1)
    top_n: int = Field(default=200, ge=10)  # overall top-N windows per period for model comparison
    seed_repeats: int = Field(default=2, ge=0)  # extra seeds per model for ranking stability
    buffer_hours_sensitivity: list[int] = [0, 24, 72, 168]
    trim_refit_quantile: float | None = Field(default=0.99, gt=0.5, lt=1)  # contamination variant


class ReportSettings(Strict):
    top_candidates: int = Field(default=25, ge=1)
    heatmap_entities: int = Field(default=30, ge=1)
    zoom_periods: int = Field(default=3, ge=1)
    zoom_hours: int = Field(default=48, ge=2)
    trace_top_n: int = Field(default=50, ge=0)  # alerts that get source file/row traces
    trace_rows_per_window: int = Field(default=5, ge=1)
    chart_format: Literal["svg", "png"] = "svg"


class SearchSettings(Strict):
    # Candidate parameter overrides per model, e.g. iforest: [{n_estimators: 100}, {max_samples: 1024}]
    iforest: list[dict] = []
    ocsvm: list[dict] = []


class PocConfig(Strict):
    name: str = Field(default="poc", pattern=r"^[A-Za-z0-9_.-]+$")
    input: InputSettings
    work_dir: Path
    annotations: Path | None = None
    annotation_buffer_hours: int = Field(default=24, ge=0)
    # The checkout must never hold real data or artifacts computed from it (ADR-012). Inputs or outputs inside
    # the repository are refused unless this is set, which asserts that they are mock/synthetic.
    allow_inside_repo: bool = False
    window_minutes: int = Field(default=60, ge=1, le=1440)
    features: FeatureSelection = FeatureSelection()
    split: SplitSettings = SplitSettings()
    models: ModelSettings = ModelSettings()
    bands: BandSettings = BandSettings()
    diagnostics: DiagnosticSettings = DiagnosticSettings()
    report: ReportSettings = ReportSettings()
    search: SearchSettings = SearchSettings()
    batch_rows: int = Field(default=100_000, ge=1_000)
    duckdb: DuckDBSettings = DuckDBSettings()

    @field_validator("window_minutes")
    @classmethod
    def _divides_day(cls, v: int) -> int:
        if 1440 % v:
            raise ValueError("window_minutes must divide 1440 so windows never span two UTC days")
        return v


_UNRESOLVED = re.compile(r"\$\{?[A-Za-z_]")


def expand_env(value: str) -> str:
    out = os.path.expandvars(value)
    if _UNRESOLVED.search(out):
        raise ValueError(f"unresolved environment variable in {value!r}")
    return out


def _anchor(value: str, base: Path) -> str:
    p = Path(value)
    return str(p if p.is_absolute() else base / p)


def load_poc_config(path: Path, overrides: dict | None = None) -> PocConfig:
    """Load YAML, expand environment variables in path fields, resolve relative paths against the file.

    `overrides` replaces top-level keys (e.g. {"work_dir": "..."}) before validation.
    """
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    data.update(overrides or {})
    base = path.resolve().parent
    if "input" not in data or "work_dir" not in data:
        raise ValueError(f"{path}: `input.paths` and `work_dir` are required")
    data["input"]["paths"] = [_anchor(expand_env(str(p)), base) for p in data["input"].get("paths") or []]
    for key in ("work_dir", "annotations"):
        if data.get(key):
            data[key] = _anchor(expand_env(str(data[key])), base)
    duck = dict(data.get("duckdb") or {})
    duck["temp_directory"] = (_anchor(expand_env(str(duck["temp_directory"])), base) if duck.get("temp_directory")
                              else str(Path(data["work_dir"]) / "tmp" / "duckdb"))
    data["duckdb"] = duck
    return PocConfig.model_validate(data)
