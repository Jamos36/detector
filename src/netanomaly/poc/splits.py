"""Chronological train / validation / test periods (whole UTC days, [start, end)). Never a random row split.

- train: the baseline the models learn from (should be a likely-clean period the user chooses);
- validation: tuning and score calibration (band cutoffs); looking at it repeatedly is expected;
- test: a later, untouched period that simulates deployment. Every time settings are changed after looking at test
  results, the test period stops being independent - the report says so next to every test figure.

Windows are assigned by `window_start`; `window_minutes` divides a day, so a window never straddles a boundary.
"""

from __future__ import annotations

import itertools
import math
from dataclasses import dataclass
from datetime import UTC, date, datetime, time, timedelta

from netanomaly.db import sql_literal
from netanomaly.poc.config import SplitSettings

PERIODS = ("train", "validation", "test")
UNASSIGNED = "unassigned"


@dataclass(frozen=True)
class Period:
    name: str
    start: date  # inclusive, 00:00 UTC
    end: date  # exclusive, 00:00 UTC

    @property
    def start_dt(self) -> datetime:
        return datetime.combine(self.start, time(), tzinfo=UTC)

    @property
    def end_dt(self) -> datetime:
        return datetime.combine(self.end, time(), tzinfo=UTC)

    def where(self, col: str = "window_start") -> str:
        return (f"({col} >= TIMESTAMPTZ {sql_literal(self.start_dt.isoformat())} AND "
                f"{col} < TIMESTAMPTZ {sql_literal(self.end_dt.isoformat())})")

    def to_dict(self) -> dict:
        return {"name": self.name, "start": self.start.isoformat(), "end_exclusive": self.end.isoformat()}


class SplitError(ValueError):
    pass


def resolve_split(settings: SplitSettings, days: list[date]) -> list[Period]:
    """Explicit periods if configured, else consecutive blocks of the days that have data (by fraction)."""
    days = sorted(set(days))
    if not days:
        raise SplitError("no feature rows: nothing to split")
    if settings.train is not None:
        periods = [Period(n, p.start, p.end) for n, p in
                   (("train", settings.train), ("validation", settings.validation), ("test", settings.test)) if p]
        method = "explicit"
    else:
        periods = _fraction_periods(days, settings.fractions)
        method = "fractions"
    _check(periods, days, method)
    return periods


def _fraction_periods(days: list[date], fractions: tuple[float, float, float]) -> list[Period]:
    n = len(days)
    n_train = max(1, math.floor(n * fractions[0]))
    n_val = math.floor(n * fractions[1])
    bounds = [0, n_train, min(n, n_train + n_val), n]
    out = []
    for name, lo, hi in zip(PERIODS, bounds, bounds[1:], strict=False):
        if hi > lo:
            out.append(Period(name, days[lo], days[hi - 1] + timedelta(days=1)))
    return out


def _check(periods: list[Period], days: list[date], method: str) -> None:
    for p in periods:
        if p.end <= p.start:
            raise SplitError(f"{p.name}: end {p.end} is not after start {p.start}")
    for a, b in itertools.pairwise(periods):
        if b.start < a.end:
            raise SplitError(f"{b.name} starts {b.start} before {a.name} ends {a.end}: periods must be "
                             "chronological and non-overlapping (train < validation < test)")
    train = periods[0]
    if not any(train.start <= d < train.end for d in days):
        raise SplitError(f"training period {train.start}..{train.end} ({method}) contains no data")


def period_case_sql(periods: list[Period], col: str = "window_start") -> str:
    whens = " ".join(f"WHEN {p.where(col)} THEN '{p.name}'" for p in periods)
    return f"CASE {whens} ELSE '{UNASSIGNED}' END"
