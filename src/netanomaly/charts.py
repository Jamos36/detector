"""Static SVG charts for the PoC report (matplotlib, Agg backend). Every function receives data that DuckDB has
already aggregated (per day / week / bucket / bin, or a bounded sample); nothing here touches raw flows.

Colours (validated with the dataviz palette checks): models are categorical slots 1-2 (blue, orange); review bands
are one ordinal blue ramp (Low light -> Critical dark); supplied pentest intervals are neutral grey shading, their
buffers lighter grey with hatching, so annotation context never looks like a model signal.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from pathlib import Path

import matplotlib

matplotlib.use("Agg")
import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import numpy as np
from matplotlib.colors import LinearSegmentedColormap
from matplotlib.patches import Patch

from netanomaly.config import BAND_ORDER

SURFACE, INK, INK_2, GRID = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e3df"
MODEL_COLORS = ["#2a78d6", "#eb6834", "#1baf7a", "#4a3aa7"]
BAND_COLORS = {"Low": "#86b6ef", "Medium": "#3987e5", "High": "#1c5cab", "Critical": "#0d366b"}
ANNOT = "#52514e"
PERIOD_STYLES = {"train": ":", "validation": "-", "test": "--", "unassigned": "-."}
SEQUENTIAL = LinearSegmentedColormap.from_list("blue_seq", ["#eef4fd", "#86b6ef", "#2a78d6", "#0d366b"])

plt.rcParams.update({
    "figure.facecolor": SURFACE, "axes.facecolor": SURFACE, "savefig.facecolor": SURFACE,
    "axes.edgecolor": GRID, "axes.labelcolor": INK_2, "axes.titlecolor": INK, "axes.titlesize": 11,
    "axes.titleweight": "bold", "axes.labelsize": 9, "xtick.color": INK_2, "ytick.color": INK_2,
    "xtick.labelsize": 8, "ytick.labelsize": 8, "legend.fontsize": 8, "legend.frameon": False,
    "axes.grid": True, "grid.color": GRID, "grid.linewidth": 0.6, "axes.spines.top": False,
    "axes.spines.right": False, "lines.linewidth": 1.6, "svg.hashsalt": "netanomaly", "font.size": 9,
})


def model_color(i: int) -> str:
    return MODEL_COLORS[i % len(MODEL_COLORS)]


def rarity(pct: np.ndarray | float, floor: float = 1e-6) -> np.ndarray:
    """-log10(1 - reference percentile): 1 = top 10 %, 2 = top 1 %, 3 = top 0.1 % of reference windows."""
    return -np.log10(np.clip(1 - np.asarray(pct, dtype=float), floor, 1))


def _save(fig: plt.Figure, path: Path, note: str) -> Path:
    fig.text(0.01, -0.02, note, fontsize=7, color=INK_2, ha="left", va="top", wrap=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    fmt = path.suffix.lstrip(".") or "svg"
    extra = {"metadata": {"Date": None}} if fmt == "svg" else {"dpi": 110}
    fig.savefig(path, format=fmt, bbox_inches="tight", **extra)
    plt.close(fig)
    return path


def shade_annotations(ax: plt.Axes, intervals: Sequence[tuple[datetime, datetime, datetime, datetime]]) -> None:
    """intervals: (buffer_start, start, end, buffer_end). Inside = grey, buffer = lighter grey + hatch."""
    for bs, s, e, be in intervals:
        ax.axvspan(bs, s, color=ANNOT, alpha=0.06, hatch="//", lw=0)
        ax.axvspan(e, be, color=ANNOT, alpha=0.06, hatch="//", lw=0)
        ax.axvspan(s, e, color=ANNOT, alpha=0.18, lw=0)


def annotation_legend() -> list[Patch]:
    return [Patch(facecolor=ANNOT, alpha=0.18, label="inside supplied pentest window (context, not a label)"),
            Patch(facecolor=ANNOT, alpha=0.06, hatch="//", label="buffer / uncertain")]


def mark_periods(ax: plt.Axes, periods: Sequence[tuple[str, datetime, datetime]], label: bool = True) -> None:
    for name, start, end in periods:
        ax.axvline(start, color=INK_2, lw=0.8, ls="--")
        ax.axvline(end, color=INK_2, lw=0.8, ls="--")
        if label:
            ax.text(start, 1.0, f" {name}", transform=ax.get_xaxis_transform(), fontsize=7, color=INK_2,
                    va="bottom")


def _date_axis(ax: plt.Axes) -> None:
    loc = mdates.AutoDateLocator(minticks=4, maxticks=10)
    ax.xaxis.set_major_locator(loc)
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(loc))


def _bar_width(xs: list[datetime]) -> float:
    if len(xs) < 2:
        return 0.02
    return 0.8 * float(np.min(np.diff(mdates.date2num(xs))))


def overview(path: Path, days: list[datetime], flows: list[float], review: dict[str, list[float]],
             intervals: list, periods: list, bucket: str) -> Path:
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 5.2), sharex=True, constrained_layout=True,
                                 gridspec_kw={"height_ratios": [1, 1.2]})
    for ax in (a1, a2):
        shade_annotations(ax, intervals)
        mark_periods(ax, periods, label=ax is a1)
    xs = [*days, days[-1] + (days[-1] - days[-2] if len(days) > 1 else timedelta(days=1))]
    a1.step(xs, [*flows, flows[-1]], where="post", color=INK_2, lw=1.2)
    a1.set_ylabel(f"flows per {bucket}")
    a1.set_title(f"Traffic volume and review-band windows per UTC {bucket}", loc="left", pad=16)
    for i, (m, ys) in enumerate(review.items()):
        a2.step(xs, [*ys, ys[-1]], where="post", color=model_color(i), label=f"{m}: windows in any review band")
    a2.set_ylabel(f"review windows per {bucket}")
    for ax in (a1, a2):
        ax.set_ylim(bottom=0)
    a2.legend(handles=[*a2.get_legend_handles_labels()[0], *annotation_legend()], loc="upper left", ncol=2)
    _date_axis(a2)
    return _save(fig, path, f"Bucket = 1 UTC {bucket}; counts aggregated in DuckDB. Review bands are rank bands "
                            "calibrated on the reference period, not severities. Grey shading is user-supplied "
                            "context, not ground truth; dashed lines are split boundaries.")


def zoom(path: Path, title: str, buckets: list[datetime], flows: list[float], pct: dict[str, list[float]],
         band_q: dict[str, float], floors: dict[str, float], intervals: list, bucket_label: str) -> Path:
    fig, (a1, a2) = plt.subplots(2, 1, figsize=(11, 4.8), sharex=True, constrained_layout=True)
    for ax in (a1, a2):
        shade_annotations(ax, intervals)
    a1.bar(buckets, flows, width=_bar_width(buckets), color=INK_2, alpha=0.55, lw=0)
    a1.set_ylabel(f"flows per {bucket_label}")
    a1.set_title(title, loc="left")
    for i, (m, ys) in enumerate(pct.items()):
        a2.plot(buckets, rarity(np.array(ys, dtype=float), floors[m]), color=model_color(i), marker="o", ms=3,
                label=f"{m}: highest window in bucket")
    for b, q in band_q.items():
        y = float(rarity(q))
        a2.axhline(y, color=BAND_COLORS[b], lw=0.9, ls=":")
        a2.text(1.0, y, f" {b}", transform=a2.get_yaxis_transform(), fontsize=7, color=INK_2, va="center")
    a1.set_ylim(bottom=0)
    a2.set_ylim(bottom=0)
    a2.set_ylabel("rarity  -log10(1 - pct)")
    a2.legend(handles=[*a2.get_legend_handles_labels()[0], *annotation_legend()], loc="upper left", ncol=2)
    _date_axis(a2)
    return _save(fig, path, f"Bucket = {bucket_label} (UTC). Dotted lines: band cutoffs (the same reference "
                            "quantiles for both models). Rarity 2 = top 1 % of reference-period windows, capped at "
                            "the reference sample's resolution -log10(1 / (n + 1)).")


def bands_over_time(path: Path, weeks: list[datetime], counts: dict[str, dict[str, list[float]]],
                    intervals: list, bucket: str, step: timedelta) -> Path:
    models = list(counts)
    fig, axes = plt.subplots(len(models), 1, figsize=(11, 2.4 * len(models) + 0.6), sharex=True,
                             constrained_layout=True, squeeze=False)
    width = 0.8 * step.total_seconds() / 86400
    for ax, m in zip(axes[:, 0], models, strict=True):
        shade_annotations(ax, intervals)
        bottom = np.zeros(len(weeks))
        for b in reversed(BAND_ORDER):
            ys = np.array(counts[m][b], dtype=float)
            ax.bar(weeks, ys, width=width, bottom=bottom, color=BAND_COLORS[b], label=b, edgecolor=SURFACE,
                   lw=0.4, align="edge")
            bottom += ys
        ax.set_ylabel(f"windows / {bucket}")
        ax.set_title(f"{m}: review-band windows per UTC {bucket}", loc="left")
    axes[0, 0].legend(loc="upper left", bbox_to_anchor=(1.0, 1.0), title="review band\n(rank, not severity)")
    _date_axis(axes[-1, 0])
    week_note = " (Monday start)" if bucket == "week" else ""
    return _save(fig, path, f"Bucket = 1 UTC {bucket}{week_note}. Bands from reference-period quantiles; the colour "
                            "ramp is ordinal (rank), not a severity scale.")


def cutoff_curve(path: Path, curve: list[dict], band_q: dict[str, dict[str, float]]) -> Path:
    models = list(dict.fromkeys(r["model"] for r in curve))
    fig, axes = plt.subplots(1, len(models), figsize=(5.2 * len(models), 3.4), constrained_layout=True,
                             squeeze=False)
    for ax, (i, m) in zip(axes[0], enumerate(models), strict=True):
        for period in sorted({r["period"] for r in curve if r["model"] == m}):
            pts = sorted((r["quantile"], r["per_day"]) for r in curve if r["model"] == m and r["period"] == period)
            ax.plot([float(rarity(q)) for q, _ in pts], [max(v, 1e-3) for _, v in pts], color=model_color(i),
                    ls=PERIOD_STYLES.get(period, "-"), marker="o", ms=3, label=period)
        for b, q in band_q[m].items():
            ax.axvline(float(rarity(q)), color=BAND_COLORS[b], lw=0.8)
            ax.text(float(rarity(q)), 1.0, f" {b}", transform=ax.get_xaxis_transform(), fontsize=7, rotation=90,
                    va="top", color=INK_2)
        ax.set_yscale("log")
        ax.set_xlabel("cutoff: reference quantile as -log10(1 - q)")
        ax.set_ylabel("windows per day at or above cutoff")
        ax.set_title(f"{m}: alert volume vs cutoff", loc="left")
        ax.legend(title="period")
    return _save(fig, path, "Stricter cutoffs (right) lower the daily volume. Validation is the calibration "
                            "target; a different test curve shows drift. Changing cutoffs needs no retraining.")


def score_distributions(path: Path, hist: dict[str, dict[str, tuple[np.ndarray, np.ndarray]]],
                        thresholds: dict[str, dict[str, float]]) -> Path:
    models = list(hist)
    fig, axes = plt.subplots(1, len(models), figsize=(5.2 * len(models), 3.4), constrained_layout=True,
                             squeeze=False)
    for ax, (i, m) in zip(axes[0], enumerate(models), strict=True):
        for period, (edges, counts) in hist[m].items():
            share = counts / max(counts.sum(), 1)
            ax.step(edges[:-1], np.where(share > 0, share, np.nan), where="post", color=model_color(i),
                    ls=PERIOD_STYLES.get(period, "-"), label=period)
        for b, t in thresholds[m].items():
            ax.axvline(t, color=BAND_COLORS[b], lw=0.9)
            ax.text(t, 1.0, f" {b}", transform=ax.get_xaxis_transform(), rotation=90, fontsize=7, va="top",
                    color=INK_2)
        ax.set_yscale("log")
        ax.set_xlabel("raw score (higher = more anomalous)")
        ax.set_ylabel("share of period's windows")
        ax.set_title(f"{m}: score distribution by period", loc="left")
        ax.legend(title="period")
    return _save(fig, path, "Raw scores are model-specific rankings (not probabilities): compare shapes across "
                            "periods, not values across models. Vertical lines: band cutoffs. 60 equal-width bins.")


def heatmap(path: Path, title: str, entities: list[str], columns: list[datetime], values: np.ndarray,
            annotated: list[bool], bucket: str, vmax: float) -> Path:
    fig, ax = plt.subplots(figsize=(11, 0.26 * len(entities) + 1.8), constrained_layout=True)
    cmap = SEQUENTIAL.with_extremes(bad=SURFACE)
    im = ax.imshow(np.ma.masked_invalid(values), aspect="auto", cmap=cmap, vmin=0, vmax=vmax,
                   interpolation="nearest")
    ax.set_yticks(range(len(entities)), entities, fontsize=7)
    step = max(1, len(columns) // 12)
    ax.set_xticks(range(0, len(columns), step), [c.strftime("%Y-%m-%d") for c in columns[::step]], rotation=45,
                  ha="right", fontsize=7)
    for j, flag in enumerate(annotated):
        if flag:
            ax.plot(j, -0.9, marker="v", color=ANNOT, ms=4, clip_on=False)
    ax.grid(False)
    ax.set_title(title, loc="left")
    fig.colorbar(im, ax=ax, label="max rarity -log10(1 - pct)", shrink=0.8)
    return _save(fig, path, f"Rows: hosts with the most review-band windows; columns: UTC {bucket}s; blank = no "
                            "traffic. Grey triangles: columns touching a supplied interval or its buffer.")


def model_scatter(path: Path, x: np.ndarray, y: np.ndarray, names: tuple[str, str], n_total: int,
                  period: str, floors: tuple[float, float]) -> Path:
    fig, ax = plt.subplots(figsize=(5.4, 4.6), constrained_layout=True)
    rx, ry = rarity(x, floors[0]), rarity(y, floors[1])
    hb = ax.hexbin(rx, ry, gridsize=40, bins="log", cmap=SEQUENTIAL, mincnt=1)
    lim = max(float(rx.max(initial=1)), float(ry.max(initial=1)))
    ax.plot([0, lim], [0, lim], color=INK_2, lw=0.8, ls=":")
    ax.set_xlabel(f"{names[0]} rarity")
    ax.set_ylabel(f"{names[1]} rarity")
    ax.set_title(f"{names[0]} vs {names[1]} ({period})", loc="left")
    fig.colorbar(hb, ax=ax, label="windows (log)")
    return _save(fig, path, f"Hexbin of {len(x):,} of {n_total:,} windows (deterministic hash sample). Windows far "
                            "from the dotted diagonal are ranked high by one model only.")


def drift(path: Path, features: list[str], weeks: list[str], psi: np.ndarray, train_weeks: list[bool],
          bucket: str) -> Path:
    fig, ax = plt.subplots(figsize=(11, 0.34 * len(features) + 1.6), constrained_layout=True)
    im = ax.imshow(psi, aspect="auto", cmap=SEQUENTIAL, vmin=0, vmax=max(0.25, float(np.nanmax(psi))),
                   interpolation="nearest")
    ax.set_yticks(range(len(features)), features, fontsize=7)
    step = max(1, len(weeks) // 14)
    ax.set_xticks(range(0, len(weeks), step), weeks[::step], rotation=45, ha="right", fontsize=7)
    for j, t in enumerate(train_weeks):
        if t:
            ax.plot(j, -0.8, marker="s", color=INK_2, ms=3, clip_on=False)
    ax.grid(False)
    ax.set_title(f"Feature drift: PSI per UTC {bucket} against the training distribution", loc="left")
    fig.colorbar(im, ax=ax, label="PSI (> 0.25: large shift by convention)", shrink=0.8)
    return _save(fig, path, f"Squares mark training {bucket}s. Drift means the traffic mix moved away from the baseline "
                            "(retraining may be needed); it is not evidence of an attack.")
