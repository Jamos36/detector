"""Feature cards (V2-5): per-feature diagnostics for the registry's usable, implemented features.

SYNTHETIC DIAGNOSTICS ONLY. Every number here comes from mock/synthetic data whose attacks we designed (ADR-012);
detection metrics are not estimates of real-world performance.

Definitions (see ARCHITECTURE.md → Feature cards):

- **Features analysed**: registry features that are usable under the contract (computed eligibility, ADR-017) and
  implemented. Usable candidates and not-usable features are listed as *not analysed* with the reason; nothing is
  computed for them. Each analysed feature is read from its table in `FEATURE_SOURCES`.
- **Rows**: the V0 host-window grid (`features/host_window`), left-joined on (`src_ip`, `window_start`) with the V2
  tables. Every table must cover exactly that grid, otherwise the cards refuse to run (stale features).
- **Distribution**: over all rows of all days: count, NULL share, min/max, mean, population std, quantiles
  (`quantile_cont`), zero share. **Cardinality**: distinct non-NULL values and the most frequent values.
- **Missingness**: NULL share overall and per day, and the row count per level of the feature's quality column
  (`baseline_quality`, `timing_quality`, novelty first-window flag) where NULLs are structural.
- **Redundancy**: Spearman rank correlation (average ranks for ties) for every pair, over the rows where both
  features are non-NULL; the row count is reported with rho.
- **PSI**: reference period = lake days `[warmup_days, warmup_days + reference_days)` by position (config
  `feature_cards`). Bins: distinct `psi_bins`-quantiles of the reference's non-NULL values as right-closed edges, plus
  one NULL bin, so a change in missingness also moves PSI. Shares are floored at `PSI_FLOOR`. Each day is compared
  with the reference. Conventional reading: < 0.1 stable, 0.1–0.25 moderate, >= 0.25 shift (rules of thumb).
- **AUROC**: see `labels.py` for label alignment. Population = host-windows on days with at least one injected flow.
  Per attack type A: positives = windows with an injected flow of A, negatives = windows with no injected flow;
  windows of other attack types are left out. "any" = all injected windows vs the same negatives. The score is the
  feature oriented by its pre-declared `Direction` (not chosen from the labels), NULL ranked below every value, ties
  counted 1/2 (Mann-Whitney). 0.5 = no separation; < 0.5 = the feature points the other way for that attack.

Memory: one TEMP TABLE of the grid (DuckDB spills past `memory_limit`); every statistic is a DuckDB aggregate, and
Python only receives aggregates (bin counts, quantiles, correlations), never rows.
"""

from __future__ import annotations

import math
from dataclasses import asdict, dataclass
from datetime import date
from enum import StrEnum
from pathlib import Path

import duckdb

from netanomaly import labels
from netanomaly.config import FeatureCardSettings
from netanomaly.db import sql_literal
from netanomaly.feature_registry import Eligibility, Registry, Status, assess
from netanomaly.features import HOST_WINDOW_FEATURES
from netanomaly.schema import Contract

REPORT_DIR = "feature_cards"
GRID = "host_window"
PSI_FLOOR = 1e-4
PSI_MODERATE, PSI_SHIFT = 0.1, 0.25
QUANTILES = (0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99)
TOP_VALUES = 3
ANY_ATTACK = "any"
BANNER = ("SYNTHETIC DIAGNOSTICS: mock/synthetic data with attacks designed by us (ADR-012). Detection metrics are "
          "not real-world performance estimates; field meanings are unvalidated (0/42), so every feature is "
          "provisional.")


class Direction(StrEnum):
    """Which end of a feature is expected to be anomalous, declared before looking at labels."""

    HIGH = "high"
    LOW = "low"


@dataclass(frozen=True)
class Source:
    table: str
    direction: Direction = Direction.HIGH
    quality: str | None = None  # SQL over the source table's alias: one category per row


_NOVELTY_QUALITY = ("CASE WHEN host_novelty.window_start = host_novelty.host_first_seen "
                    "THEN 'host_first_window' ELSE 'has_history' END")
FEATURE_SOURCES: dict[str, Source] = {
    **{name: Source(GRID) for name in HOST_WINDOW_FEATURES},
    "bytes_out_robust_z": Source("host_baseline", quality="host_baseline.baseline_quality"),
    "new_dst_ip_rate": Source("host_novelty", quality=_NOVELTY_QUALITY),
    "new_dst_port_rate": Source("host_novelty", quality=_NOVELTY_QUALITY),
    "interarrival_cv": Source("host_timing", Direction.LOW, quality="host_timing.timing_quality"),
}
COMMANDS = {GRID: "features", "host_baseline": "baselines", "host_novelty": "novelty", "host_timing": "timing"}


@dataclass(frozen=True)
class NotAnalysed:
    name: str
    status: str
    reason: str


@dataclass(frozen=True)
class Distribution:
    rows: int
    non_null: int
    null_rate: float
    distinct: int
    zero_share: float | None
    min: float | None
    max: float | None
    mean: float | None
    std: float | None
    quantiles: dict[str, float | None]


@dataclass(frozen=True)
class ValueCount:
    value: str
    rows: int
    share: float


@dataclass(frozen=True)
class PsiDay:
    flow_date: date
    role: str  # warm-up | reference | compare
    rows: int
    null_rate: float
    psi: float | None
    reading: str


@dataclass(frozen=True)
class Auroc:
    attack_type: str
    positives: int
    negatives: int
    positives_non_null: int
    auroc: float | None


@dataclass(frozen=True)
class Correlation:
    other: str
    rho: float | None
    rows: int


@dataclass(frozen=True)
class Card:
    name: str
    table: str
    temporal_scope: str
    direction: str
    sources: list[str]
    attack_hypotheses: list[str]
    distribution: Distribution
    top_values: list[ValueCount]
    quality: list[ValueCount]
    psi: list[PsiDay]
    auroc: list[Auroc]
    correlations: list[Correlation]


@dataclass(frozen=True)
class LabelSummary:
    attack_type: str
    windows: int
    injected_flows: int
    median_purity: float | None
    min_purity: float | None


@dataclass(frozen=True)
class Report:
    banner: str
    registry_version: int
    window_minutes: int
    settings: dict
    days: list[date]
    reference_days: list[date]
    eval_days: list[date]
    rows: int
    truth: dict | None
    labels: list[LabelSummary]
    cards: list[Card]
    not_analysed: list[NotAnalysed]
    redundant_pairs: list[tuple[str, str, float]]


# --- feature selection ---------------------------------------------------------------------------------------

def select_features(registry: Registry, contract: Contract) -> tuple[list[Eligibility], list[NotAnalysed]]:
    """(usable implemented features, every other registry feature with the reason it is not analysed)."""
    chosen, skipped = [], []
    for e in assess(registry, contract):
        f = e.feature
        if not e.eligible:
            skipped.append(NotAnalysed(f.name, str(f.status), "not usable: " + "; ".join(e.reasons)))
        elif f.status is Status.CANDIDATE:
            skipped.append(NotAnalysed(f.name, str(f.status),
                                       f"usable candidate, not implemented ({f.task or 'unscheduled'})"))
        elif f.name not in FEATURE_SOURCES:
            raise ValueError(f"{f.name}: implemented and usable but has no feature_cards.FEATURE_SOURCES entry")
        else:
            chosen.append(e)
    return chosen, skipped


# --- the analysis table --------------------------------------------------------------------------------------

def _table(features_dir: Path, table: str) -> str:
    part = features_dir / table
    if not any(part.rglob("*.parquet")):
        raise FileNotFoundError(f"{part} has no Parquet files; run `netanomaly {COMMANDS[table]}` first")
    return f"read_parquet({sql_literal(part / '**' / '*.parquet')}, hive_partitioning = true)"


def _check_coverage(con: duckdb.DuckDBPyConnection, features_dir: Path, tables: list[str], rows: int,
                    labels_sql: str | None) -> None:
    for t in tables:
        (table_rows,) = con.execute(f"SELECT count(*) FROM {_table(features_dir, t)}").fetchone()
        (on_grid,) = con.execute(f"SELECT count(*) FILTER (WHERE _in_{t}) FROM card_rows").fetchone()
        if not table_rows == on_grid == rows:
            raise ValueError(f"{t} ({table_rows} rows, {on_grid} on the grid) does not cover the {rows}-row "
                             f"{GRID} grid; rebuild it with `netanomaly {COMMANDS[t]}`")
    if labels_sql:
        (label_rows,) = con.execute(f"SELECT count(*) FROM ({labels_sql})").fetchone()
        (on_grid,) = con.execute("SELECT count(*) FILTER (WHERE attack_types IS NOT NULL) FROM card_rows").fetchone()
        if label_rows != on_grid:
            raise ValueError(f"{label_rows - on_grid} injected host-windows are not on the {GRID} grid")


def create_card_rows(con: duckdb.DuckDBPyConnection, features_dir: Path, names: list[str],
                     labels_sql: str | None) -> int:
    """Materialise TEMP TABLE card_rows: the grid, one column per feature, `<name>__q`, labels. Returns row count."""
    tables = sorted({FEATURE_SOURCES[n].table for n in names} - {GRID})
    joins = "".join(f"\nLEFT JOIN {_table(features_dir, t)} AS {t} "
                    f"ON {t}.src_ip = {GRID}.src_ip AND {t}.window_start = {GRID}.window_start" for t in tables)
    cols = [f'{FEATURE_SOURCES[n].table}."{n}" AS "{n}"' for n in names]
    cols += [f'{FEATURE_SOURCES[n].quality} AS "{n}__q"' for n in names if FEATURE_SOURCES[n].quality]
    cols += [f"{t}.src_ip IS NOT NULL AS _in_{t}" for t in tables]
    label_cols, label_join = "NULL::VARCHAR[] AS attack_types, 0 AS injected_flows", ""
    if labels_sql:
        label_cols = "lab.attack_types, coalesce(lab.injected_flows, 0) AS injected_flows"
        label_join = (f"\nLEFT JOIN ({labels_sql}) lab "
                      f"ON lab.src_ip = {GRID}.src_ip AND lab.window_start = {GRID}.window_start")
    con.execute(f"""
CREATE OR REPLACE TEMP TABLE card_rows AS
SELECT {GRID}.src_ip, {GRID}.window_start, {GRID}.flow_date, {GRID}.flows AS grid_flows, {', '.join(cols)},
       {label_cols}
FROM {_table(features_dir, GRID)} AS {GRID}{joins}{label_join}""")
    (rows,) = con.execute("SELECT count(*) FROM card_rows").fetchone()
    _check_coverage(con, features_dir, tables, rows, labels_sql)
    return rows


# --- per-feature statistics ----------------------------------------------------------------------------------

def distribution(con: duckdb.DuckDBPyConnection, name: str) -> Distribution:
    x = f'"{name}"'
    rows, non_null, distinct, zeros, lo, hi, mean, std, qs = con.execute(f"""
SELECT count(*), count({x}), count(DISTINCT {x}), count(*) FILTER (WHERE {x} = 0), min({x})::DOUBLE,
       max({x})::DOUBLE, avg({x}), stddev_pop({x}), quantile_cont({x}::DOUBLE, {list(QUANTILES)})
FROM card_rows""").fetchone()
    quantiles = dict(zip((f"p{round(q * 100):02d}" for q in QUANTILES), qs or [None] * len(QUANTILES), strict=True))
    return Distribution(rows, non_null, 1 - non_null / rows if rows else 0.0, distinct,
                        zeros / non_null if non_null else None, lo, hi, mean, std, quantiles)


def value_counts(con: duckdb.DuckDBPyConnection, expr: str, limit: int | None = None) -> list[ValueCount]:
    """Rows per value of `expr` over card_rows, most frequent first (ties by value); NULL shown as 'NULL'."""
    rows = con.execute(f"""
SELECT coalesce(v::VARCHAR, 'NULL'), n, n / sum(n) OVER () FROM (
  SELECT {expr} AS v, count(*) AS n FROM card_rows GROUP BY ALL)
ORDER BY n DESC, 1 {f'LIMIT {int(limit)}' if limit else ''}""").fetchall()
    return [ValueCount(v, n, share) for v, n, share in rows]


def psi(expected: list[int], actual: list[int], floor: float = PSI_FLOOR) -> float:
    """Population stability index of `actual` against `expected` bin counts; shares below `floor` are floored."""
    if len(expected) != len(actual) or not sum(expected) or not sum(actual):
        raise ValueError("psi needs two non-empty count vectors of equal length")
    te, ta = sum(expected), sum(actual)
    total = 0.0
    for e, a in zip(expected, actual, strict=True):
        pe, pa = max(e / te, floor), max(a / ta, floor)
        total += (pa - pe) * math.log(pa / pe)
    return total


def psi_reading(value: float | None) -> str:
    if value is None:
        return "n/a"
    return "stable" if value < PSI_MODERATE else "moderate" if value < PSI_SHIFT else "shift"


def _date_list(days: list[date]) -> str:
    return ", ".join(f"DATE '{d.isoformat()}'" for d in days)


def psi_bin_counts(con: duckdb.DuckDBPyConnection, name: str, reference: list[date],
                   bins: int) -> tuple[list[float], dict[date, dict[int, int]]]:
    """Reference-quantile edges and rows per (day, bin); bin -1 = NULL, bin k = #edges below the value."""
    x = f'"{name}"'
    (edges,) = con.execute(f"""
SELECT list(DISTINCT q ORDER BY q) FROM (
  SELECT unnest(quantile_disc({x}::DOUBLE, {[i / bins for i in range(1, bins)]})) AS q
  FROM card_rows WHERE flow_date IN ({_date_list(reference)}) AND {x} IS NOT NULL)""").fetchone()
    edges = edges or []
    counts: dict[date, dict[int, int]] = {}
    for d, b, n in con.execute(f"""
SELECT flow_date, CASE WHEN {x} IS NULL THEN -1
                       ELSE len(list_filter(CAST($edges AS DOUBLE[]), lambda e: e < {x})) END, count(*)
FROM card_rows GROUP BY ALL""", {"edges": edges}).fetchall():
        counts.setdefault(d, {})[b] = n
    return edges, counts


def psi_by_day(con: duckdb.DuckDBPyConnection, name: str, days: list[date], reference: list[date],
               bins: int) -> list[PsiDay]:
    """PSI of every day against the reference days (reference-quantile bins + a NULL bin)."""
    edges, counts = psi_bin_counts(con, name, reference, bins)
    slots = range(-1, len(edges) + 1)
    vectors = {d: [counts.get(d, {}).get(b, 0) for b in slots] for d in days}
    ref = [sum(vectors[d][i] for d in reference) for i in range(len(slots))]
    out = []
    for d in days:
        vec, n = vectors[d], sum(vectors[d])
        value = psi(ref, vec) if n else None
        role = "reference" if d in reference else "warm-up" if d < reference[0] else "compare"
        out.append(PsiDay(d, role, n, vec[0] / n if n else 0.0, value, psi_reading(value)))
    return out


def auroc(con: duckdb.DuckDBPyConnection, rows_sql: str, score: str, label: str) -> tuple[float | None, int, int]:
    """Mann-Whitney AUROC of `score` (higher = more anomalous) for boolean `label` over the rows of `rows_sql`.

    NULL scores rank below every value and tie with each other; tied scores count 1/2. Returns (auc, pos, neg);
    auc is None without positives or negatives.
    """
    auc, pos, neg = con.execute(f"""
WITH d AS (SELECT {score} AS s, ({label})::BOOLEAN AS y FROM ({rows_sql})),
g AS (SELECT s, count(*) FILTER (WHERE y) AS p, count(*) FILTER (WHERE NOT y) AS n FROM d GROUP BY s),
c AS (SELECT p, n, coalesce(sum(n) OVER (ORDER BY s ASC NULLS FIRST
                                          ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING), 0) AS below FROM g)
SELECT sum(p * (below + n / 2.0)) / nullif(sum(p) * sum(n), 0), coalesce(sum(p), 0), coalesce(sum(n), 0)
FROM c""").fetchone()
    return (float(auc) if auc is not None else None), int(pos), int(neg)


def auroc_by_attack(con: duckdb.DuckDBPyConnection, name: str, direction: Direction, attack_types: list[str],
                    eval_days: list[date]) -> list[Auroc]:
    x = f'"{name}"'
    score = x if direction is Direction.HIGH else f"-{x}"
    out = []
    for attack in [ANY_ATTACK, *attack_types]:
        keep = ("TRUE" if attack == ANY_ATTACK
                else f"attack_types IS NULL OR list_contains(attack_types, {sql_literal(attack)})")
        rows_sql = f"SELECT * FROM card_rows WHERE flow_date IN ({_date_list(eval_days)}) AND ({keep})"
        auc, pos, neg = auroc(con, rows_sql, score, "attack_types IS NOT NULL")
        (pos_non_null,) = con.execute(f"SELECT count({x}) FROM ({rows_sql}) WHERE attack_types IS NOT NULL").fetchone()
        out.append(Auroc(attack, pos, neg, pos_non_null, auc))
    return out


def _avg_rank(name: str) -> str:
    return f'rank() OVER (ORDER BY "{name}") + (count(*) OVER (PARTITION BY "{name}") - 1) / 2.0'


def spearman(con: duckdb.DuckDBPyConnection, names: list[str]) -> dict[tuple[str, str], tuple[float | None, int]]:
    """Spearman rho (average ranks) over pairwise-complete rows, and that row count, for every pair of features.

    Columns without NULLs share one rank table; a pair involving a column with NULLs is re-ranked on its complete
    rows, so rho is exact in both cases.
    """
    pairs = [(a, b) for i, a in enumerate(names) for b in names[i + 1:]]
    if not pairs:
        return {}
    nulls = con.execute("SELECT " + ", ".join(f'count(*) - count("{n}")' for n in names) + " FROM card_rows").fetchone()
    complete = [n for n, k in zip(names, nulls, strict=True) if k == 0]
    out: dict[tuple[str, str], tuple[float | None, int]] = {}
    if len(complete) > 1:
        ranks = ", ".join(f'{_avg_rank(n)} AS "{n}"' for n in complete)
        shared = [(a, b) for a, b in pairs if a in complete and b in complete]
        exprs = ", ".join(f'corr("{a}", "{b}")' for a, b in shared)
        values = con.execute(f"SELECT {exprs}, count(*) FROM (SELECT {ranks} FROM card_rows)").fetchone()
        out.update({pair: (values[i], values[-1]) for i, pair in enumerate(shared)})
    for a, b in pairs:
        if (a, b) not in out:
            out[(a, b)] = con.execute(f"""
SELECT corr(ra, rb), count(*) FROM (
  SELECT {_avg_rank(a)} AS ra, {_avg_rank(b)} AS rb FROM card_rows WHERE "{a}" IS NOT NULL AND "{b}" IS NOT NULL)
""").fetchone()
    # corr() is NaN when a feature is constant on the rows: rho is undefined there, reported as None
    return {pair: (None if out[pair][0] is None or math.isnan(out[pair][0]) else out[pair][0], int(out[pair][1]))
            for pair in pairs}


# --- assembly --------------------------------------------------------------------------------------------------

def label_summary(con: duckdb.DuckDBPyConnection) -> list[LabelSummary]:
    """Injected host-windows per attack type and their purity (injected / all flows of the window)."""
    rows = con.execute("""
SELECT attack_type, count(*), sum(injected_flows)::BIGINT, median(injected_flows / grid_flows),
       min(injected_flows / grid_flows)
FROM (SELECT unnest(attack_types) AS attack_type, injected_flows, grid_flows FROM card_rows
      WHERE attack_types IS NOT NULL)
GROUP BY ALL ORDER BY 1""").fetchall()
    return [LabelSummary(*r) for r in rows]


def _correlations(name: str, corr: dict[tuple[str, str], tuple[float | None, int]]) -> list[Correlation]:
    out = [Correlation(b if a == name else a, rho, n) for (a, b), (rho, n) in corr.items() if name in (a, b)]
    return sorted(out, key=lambda c: (-abs(c.rho) if c.rho is not None else 1.0, c.other))


def _card(con: duckdb.DuckDBPyConnection, e: Eligibility, days: list[date], reference: list[date],
          eval_days: list[date], attack_types: list[str], corr: dict, s: FeatureCardSettings) -> Card:
    name, src = e.feature.name, FEATURE_SOURCES[e.feature.name]
    return Card(
        name=name, table=src.table, temporal_scope=str(e.feature.temporal_scope), direction=str(src.direction),
        sources=[f"{c.name} ({c.confidence}, {'validated' if c.validated else 'unvalidated'})" for c in e.sources],
        attack_hypotheses=[f"{h.technique} {h.name}" for h in e.feature.attack_hypotheses],
        distribution=distribution(con, name),
        top_values=value_counts(con, f'"{name}"', TOP_VALUES),
        quality=value_counts(con, f'"{name}__q"') if src.quality else [],
        psi=psi_by_day(con, name, days, reference, s.psi_bins) if reference else [],
        auroc=auroc_by_attack(con, name, src.direction, attack_types, eval_days) if eval_days else [],
        correlations=_correlations(name, corr),
    )


def build_cards(con: duckdb.DuckDBPyConnection, registry: Registry, contract: Contract, lake: Path,
                features_dir: Path, truth_dir: Path, window_minutes: int, s: FeatureCardSettings) -> Report:
    """Cards for every usable implemented feature. Truth is used only when present and verified against the lake."""
    chosen, skipped = select_features(registry, contract)
    names = [e.feature.name for e in chosen]
    truth = labels.verify_truth(con, lake, truth_dir) if labels.has_truth(truth_dir) else None
    labels_sql = labels.window_labels_sql(lake, truth_dir, window_minutes) if truth else None
    rows = create_card_rows(con, features_dir, names, labels_sql)
    days = [d for (d,) in con.execute("SELECT DISTINCT flow_date FROM card_rows ORDER BY 1").fetchall()]
    reference = days[s.warmup_days:s.warmup_days + s.reference_days]
    reference = reference if len(reference) == s.reference_days else []
    eval_days = [d for d in (truth.attack_days if truth else ()) if d in days]
    summary = label_summary(con) if truth else []
    corr = spearman(con, names)
    cards = [_card(con, e, days, reference, eval_days, [ls.attack_type for ls in summary], corr, s)
             for e in chosen]
    redundant = sorted((a, b, rho) for (a, b), (rho, _) in corr.items()
                       if rho is not None and abs(rho) >= s.redundancy_threshold)
    return Report(BANNER, registry.registry_version, window_minutes, s.model_dump(), days, reference, eval_days,
                  rows, asdict(truth) if truth else None, summary, cards, skipped, redundant)

