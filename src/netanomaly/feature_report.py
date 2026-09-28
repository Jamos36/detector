"""Render feature cards (V2-5) as `outputs/feature_cards/feature_cards.{json,md}`, like the DQ report."""

from __future__ import annotations

import json
from dataclasses import asdict
from pathlib import Path

from netanomaly.feature_cards import ANY_ATTACK, PSI_MODERATE, PSI_SHIFT, Card, Report

JSON_FILE, MD_FILE = "feature_cards.json", "feature_cards.md"
# DuckDB's parallel aggregates vary in the last float digits between runs; rounding keeps reruns byte-identical.
JSON_SIGNIFICANT_DIGITS = 10


def rounded(obj: object, digits: int = JSON_SIGNIFICANT_DIGITS) -> object:
    """Copy of a JSON-ready structure with every float rounded to `digits` significant digits."""
    if isinstance(obj, float):
        return float(f"{obj:.{digits}g}")
    if isinstance(obj, dict):
        return {k: rounded(v, digits) for k, v in obj.items()}
    if isinstance(obj, list | tuple):
        return [rounded(v, digits) for v in obj]
    return obj


def _f(v: float | None, digits: int = 4) -> str:
    if v is None:
        return "—"
    if isinstance(v, int):
        return f"{v:,}"
    return f"{v:.{digits}g}"


def _pct(v: float | None) -> str:
    return "—" if v is None else f"{100 * v:.1f}%"


def _auroc(card: Card, attack: str) -> str:
    return next((_f(a.auroc, 3) for a in card.auroc if a.attack_type == attack), "—")


def _summary(r: Report) -> list[str]:
    attacks = [ls.attack_type for ls in r.labels]
    head = ["feature", "table", "anomalous end", "NULL", "distinct", "max PSI (compare days)", "top \\|rho\\|",
            f"AUROC {ANY_ATTACK}", *attacks]
    lines = ["| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
    for c in r.cards:
        compare = [p for p in c.psi if p.role == "compare" and p.psi is not None]
        worst = max(compare, key=lambda p: p.psi, default=None)
        top = c.correlations[0] if c.correlations else None
        cells = [f"`{c.name}`", c.table, c.direction, _pct(c.distribution.null_rate), _f(c.distribution.distinct),
                 f"{_f(worst.psi, 3)} ({worst.flow_date}, {worst.reading})" if worst else "—",
                 f"{_f(top.rho, 3)} `{top.other}`" if top else "—", _auroc(c, ANY_ATTACK),
                 *(_auroc(c, a) for a in attacks)]
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _methods(r: Report) -> list[str]:
    s = r.settings
    ref = ", ".join(map(str, r.reference_days)) or "none (lake too short)"
    return [
        "## Methods",
        "",
        f"- **Rows**: {r.rows:,} host-windows ({r.window_minutes}-minute V0 grid, `features/host_window`), joined "
        "on (`src_ip`, `window_start`) with `host_baseline`, `host_novelty`, `host_timing`; each table was checked "
        "to cover exactly this grid. Days: " + ", ".join(map(str, r.days)) + ".",
        ("- **Features analysed**: registry features that are usable under the contract (computed eligibility) *and* "
        "implemented. Everything else is listed under *Not analysed*; no statistic was computed for it."),
        ("- **Distribution / cardinality / missingness**: all rows of all days, including warm-up and attack days. "
        "Quantiles interpolated (`quantile_cont`). Quality levels show *why* values are NULL where NULL is structural."),
        (f"- **Redundancy**: Spearman rho per pair (average ranks; pairwise-complete rows). |rho| ≥ "
        f"{s['redundancy_threshold']} is flagged."),
        (f"- **PSI reference period**: {ref} = lake days [{s['warmup_days']}, {s['warmup_days'] + s['reference_days']})"
        " by position (config `feature_cards`). The first days are warm-up for prior-history features (baselines "
        "need 2 earlier days; novelty's first day is every host's first window). The choice is positional, not "
        f"label-based. Bins: {s['psi_bins']}-quantiles of the reference's non-NULL values (distinct, right-closed) "
        f"plus a NULL bin; shares floored at 1e-4. Reading (rule of thumb): < {PSI_MODERATE} stable, "
        f"{PSI_MODERATE}–{PSI_SHIFT} moderate, ≥ {PSI_SHIFT} shift. Attack days contain the injected windows "
        "(a tiny share of rows)."),
        ("- **Label alignment**: a *flow* is positive when it is listed in `truth/injected_flows.csv` (synthetic key, "
        "ADR-016); no flow-level feature is implemented, so none is scored. A *host-window* is positive for attack A "
        "when it contains ≥ 1 injected flow of A, negative only when it contains no injected flow. Positive windows "
        "may also contain the host's benign flows (purity below). Prior-history features describe data *before* the "
        "window, so they can lag the label (timing: the 2 h before; baselines: earlier days)."),
        "- **AUROC**: population = host-windows on days with injections ("
        + (", ".join(map(str, r.eval_days)) or "none") + "). "
        "Per attack type: its windows vs windows with no injected flow (other attacks left out); `any` = all injected "
        "windows. Score = the feature in its pre-declared anomalous direction (not fitted to labels); NULL ranks "
        "lowest; ties count 1/2. 0.5 = no separation, < 0.5 = the feature points the other way for that attack.",
        "",
    ]


def _truth(r: Report) -> list[str]:
    if r.truth is None:
        return ["## Truth-to-lake mapping", "", "No truth files: detection metrics skipped.", ""]
    t = r.truth
    return [
        "## Truth-to-lake mapping (verified before use)",
        "",
        "| check | value |",
        "|---|---|",
        f"| injections in `injections.csv` | {t['injections']} |",
        f"| truth flows / matched to exactly one lake flow | {t['truth_flows']:,} / {t['matched_flows']:,} |",
        f"| truth flows with an unknown injection_id | {t['unknown_injections']} |",
        f"| lake src_ip ≠ injection src_ip | {t['src_mismatches']} |",
        f"| injections whose lake flow count ≠ n_flows | {t['count_mismatches']} |",
        f"| days with injected flows | {', '.join(map(str, t['attack_days']))} |",
        "",
        "Injected host-windows (labels) and purity = injected flows / all flows of the window:",
        "",
        "| attack type | windows | injected flows | median purity | min purity |",
        "|---|---|---|---|---|",
        *(f"| {ls.attack_type} | {ls.windows} | {ls.injected_flows:,} | {_pct(ls.median_purity)} "
          f"| {_pct(ls.min_purity)} |" for ls in r.labels),
        "",
    ]


def _card(c: Card) -> list[str]:
    d = c.distribution
    lines = [
        f"### `{c.name}`",
        "",
        f"Table `{c.table}` · scope {c.temporal_scope} · anomalous end: **{c.direction}** · sources: "
        + ", ".join(c.sources) + " — provisional."
        + (f" ATT&CK hypotheses (not evidence): {', '.join(c.attack_hypotheses)}." if c.attack_hypotheses else ""),
        "",
        "| rows | non-NULL | NULL | distinct | zero share | min | " + " | ".join(d.quantiles) + " | max | mean | std |",
        "|" + "---|" * (9 + len(d.quantiles)),
        f"| {d.rows:,} | {d.non_null:,} | {_pct(d.null_rate)} | {d.distinct:,} | {_pct(d.zero_share)} | {_f(d.min)} | "
        + " | ".join(_f(v) for v in d.quantiles.values()) + f" | {_f(d.max)} | {_f(d.mean)} | {_f(d.std)} |",
        "",
        "Most frequent values: " + ", ".join(f"`{v.value}` {_pct(v.share)}" for v in c.top_values) + ".",
    ]
    if c.quality:
        lines.append("Quality levels: " + ", ".join(f"`{v.value}` {v.rows:,} ({_pct(v.share)})" for v in c.quality)
                     + ".")
    lines.append("")
    if c.psi:
        lines += ["| day | role | rows | NULL | PSI | reading |", "|---|---|---|---|---|---|",
                  *(f"| {p.flow_date} | {p.role} | {p.rows:,} | {_pct(p.null_rate)} | {_f(p.psi, 3)} | {p.reading} |"
                    for p in c.psi), ""]
    if c.auroc:
        lines += ["| attack (synthetic) | positives | non-NULL positives | negatives | AUROC |",
                  "|---|---|---|---|---|",
                  *(f"| {a.attack_type} | {a.positives} | {a.positives_non_null} | {a.negatives:,} "
                    f"| {_f(a.auroc, 3)} |" for a in c.auroc), ""]
    lines += ["Most correlated: " + ", ".join(f"`{x.other}` {_f(x.rho, 3)}" for x in c.correlations[:3]) + ".", ""]
    return lines


def _limitations() -> list[str]:
    return [
        "## Limitations",
        "",
        ("- Synthetic diagnostics only: attacks are few (one per type per attack day), loud and designed by us; "
        "AUROC here says whether a feature *can* separate these injections, not how it performs on real traffic."),
        "- Single-feature AUROC ignores the alert budget (top-K/day) and interactions between features.",
        "- Direction is declared a priori; a feature with AUROC < 0.5 for an attack is still reported as is.",
        ("- Label = window containing injected flows. Lagging prior-history features (timing, baselines) are "
        "penalised for windows before the signal builds up and after the attack stops."),
        ("- PSI depends on the reference choice and on bins from a single day; discrete features with few distinct "
        "values get few bins. Clean synthetic days are stationary by construction."),
        ("- `any` pools all injected windows, so it is dominated by the attack with the most windows (beaconing); "
        "read the per-type columns. For mostly-NULL features (`interarrival_cv`), AUROC mixes coverage (NULL ranks "
        "lowest) with separation; see the non-NULL positives column."),
        "- Spearman correlations of features with many NULLs rest on the rows where both are present.",
        "- Field meanings are unvalidated (0/42 contract fields), so every feature and number here is provisional.",
        "",
    ]


def to_markdown(r: Report) -> str:
    lines = [
        "# Feature cards (V2-5)",
        "",
        f"> **{r.banner}**",
        "",
        (f"Registry version {r.registry_version}; {len(r.cards)} analysed features, {len(r.not_analysed)} not "
        "analysed. Generated by `uv run netanomaly [--root DIR] feature-cards`."),
        "",
        "## Summary (AUROC columns: synthetic diagnostics)",
        "",
        *_summary(r),
        "",
        "Redundant pairs: " + (", ".join(f"`{a}`–`{b}` {_f(rho, 3)}" for a, b, rho in r.redundant_pairs) or "none")
        + ".",
        "",
        "## Not analysed",
        "",
        "| feature | status | reason |",
        "|---|---|---|",
        *(f"| `{n.name}` | {n.status} | {n.reason} |" for n in r.not_analysed),
        "",
        *_methods(r),
        *_truth(r),
        "## Cards",
        "",
    ]
    for c in r.cards:
        lines += _card(c)
    return "\n".join(lines + _limitations())


def write_report(report: Report, out_dir: Path) -> tuple[Path, Path]:
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path, md_path = out_dir / JSON_FILE, out_dir / MD_FILE
    json_path.write_text(json.dumps(rounded(asdict(report)), indent=2, default=str) + "\n", encoding="utf-8")
    md_path.write_text(to_markdown(report), encoding="utf-8")
    return json_path, md_path
