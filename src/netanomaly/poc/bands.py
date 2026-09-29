"""Review bands: Critical / High / Medium / Low / <below_label> (default "Benign").

These are RANK bands for analyst review, not probabilities, calibrated risk or ground-truth severity. A band cutoff
is a raw-score threshold taken from the model's own scores on a reference period (validation by default):
- quantile mode: cutoff = the q-quantile of reference scores (e.g. High = 0.995 -> ~0.5% of reference windows);
- budget mode: q = 1 - budget_per_day / mean windows per day on the reference period, i.e. about `budget` windows
  per day would reach the band if the scored period looked like the reference period.
`Benign` means "below the configured review threshold"; it does not mean the activity was safe.

Bands depend only on stored raw scores and the reference period, so they can be changed without retraining
(`netanomaly poc report --bands FILE`). Changing features, windows or model parameters needs a new experiment.
`score_pct` is the share of reference windows scoring at or below a window (resolution 0.001, 0.00001 above 0.99).
"""

from __future__ import annotations

from dataclasses import asdict, dataclass

import duckdb
import numpy as np

from netanomaly.poc.config import BAND_ORDER, BandSettings

MAX_REFERENCE_ROWS = 5_000_000  # larger reference periods use a deterministic hash sample of this size
PCT_GRID = np.unique(np.concatenate([np.linspace(0, 0.99, 991), np.linspace(0.99, 1, 1001)]))


class BandError(ValueError):
    pass


@dataclass
class Cutoffs:
    model_id: str
    reference: str
    reference_rows: int
    windows_per_day: float
    mode: str
    quantiles: dict[str, float]  # band -> reference quantile
    thresholds: dict[str, float]  # band -> raw score cutoff (>= is in the band)
    below_label: str

    def to_dict(self) -> dict:
        return asdict(self)


def band_quantiles(settings: BandSettings, windows_per_day: float) -> dict[str, float]:
    if settings.mode == "quantile":
        return dict(settings.quantiles)
    out = {}
    for band in BAND_ORDER:
        budget = settings.budget_per_day[band]
        if budget >= windows_per_day:
            raise BandError(f"budget {budget}/day for {band} >= {windows_per_day:.1f} reference windows per day")
        out[band] = 1 - budget / windows_per_day
    return out


def calibrate(model_id: str, reference_scores: np.ndarray, windows_per_day: float, settings: BandSettings,
              reference_name: str) -> Cutoffs:
    scores = np.asarray(reference_scores, dtype=np.float64)
    scores = scores[np.isfinite(scores)]
    if len(scores) < 10:
        raise BandError(f"{model_id}: only {len(scores)} reference scores in {reference_name}; need >= 10")
    qs = band_quantiles(settings, windows_per_day)
    # method='higher' picks an observed score, so "raw >= cutoff" is exactly the windows at or above it
    thresholds = {b: float(np.quantile(scores, q, method="higher")) for b, q in qs.items()}
    return Cutoffs(model_id, reference_name, len(scores), windows_per_day, settings.mode, qs, thresholds,
                   settings.below_label)


def band_sql(raw_col: str, cutoffs: Cutoffs) -> str:
    whens = " ".join(f"WHEN {raw_col} >= {cutoffs.thresholds[b]!r} THEN '{b}'" for b in BAND_ORDER)
    return f"CASE {whens} ELSE '{cutoffs.below_label}' END"


def reference_scores(con: duckdb.DuckDBPyConnection, scores_rel: str, raw_col: str,
                     where: str) -> tuple[np.ndarray, float]:
    """Raw scores of the reference rows (hash-sampled above MAX_REFERENCE_ROWS) and mean windows per day."""
    n, days = con.execute(f"SELECT count(*), count(DISTINCT CAST(window_start AS DATE)) FROM {scores_rel} "
                          f"WHERE {where}").fetchone()
    limit = f"ORDER BY hash(src_ip, window_start) LIMIT {MAX_REFERENCE_ROWS}" if n > MAX_REFERENCE_ROWS else ""
    arr = con.execute(f"SELECT {raw_col} FROM {scores_rel} WHERE {where} {limit}").fetchnumpy()[raw_col]
    return np.asarray(arr, dtype=np.float64), (n / days if days else 0.0)


def percentile_grid(reference: np.ndarray) -> list[tuple[float, float]]:
    """(score, share of reference <= score) pairs for an ASOF join; ties keep one row per score."""
    ref = np.sort(reference[np.isfinite(reference)])
    values = np.unique(np.quantile(ref, PCT_GRID, method="lower"))
    shares = np.searchsorted(ref, values, side="right") / len(ref)
    return [(float(v), float(s)) for v, s in zip(values, shares, strict=True)]


def bands_markdown(cutoffs: list[Cutoffs]) -> str:
    head = "| model | reference | rows | mode | " + " | ".join(f"{b}: q / raw >=" for b in BAND_ORDER) + " |"
    lines = [head, "|" + "---|" * (4 + len(BAND_ORDER))]
    for c in cutoffs:
        cells = " | ".join(f"{c.quantiles[b]:.5f} / {c.thresholds[b]:.5g}" for b in BAND_ORDER)
        lines.append(f"| {c.model_id} | {c.reference} | {c.reference_rows:,} | {c.mode} | {cells} |")
    return "\n".join(lines)
