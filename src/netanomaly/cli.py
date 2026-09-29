"""Command-line entry point: `uv run netanomaly <command>`.

Each stage reads the previous stage's files from disk, so any stage can be
re-run on its own. `--root DIR` points every path at DIR/{raw,lake,...},
which keeps separate datasets (mock vs synthetic) in separate lakes.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
from datetime import date, timedelta
from pathlib import Path

import joblib

from netanomaly import (
    alerts,
    baselines,
    feature_cards,
    feature_registry,
    feature_report,
    features,
    iforest,
    labels,
    novelty,
    quality,
    stability,
    timing,
)
from netanomaly.config import Paths, Settings, load_settings
from netanomaly.db import connect
from netanomaly.ingest import ingest_directory
from netanomaly.inject import InjectionLog, make_injector
from netanomaly.schema import load_contract
from netanomaly.synth import generate

log = logging.getLogger("netanomaly")
SCORES_FILE = "scores.parquet"
ALERTS_FILE = "top_alerts.csv"


def _settings(args: argparse.Namespace) -> Settings:
    s = load_settings(Path(args.config))
    if args.root:
        root = Path(args.root).resolve()
        s = s.model_copy(update={"paths": Paths(**{k: root / k for k in Paths.model_fields})})
    return s


def _host_window_dir(s: Settings) -> Path:
    return s.paths.features / "host_window"


def _truth_dir(s: Settings) -> Path:
    return s.paths.raw.parent / "truth"


def _latest_model(s: Settings) -> tuple[object, iforest.ModelManifest]:
    versions = sorted(s.paths.models.glob("iforest-*"))
    if not versions:
        raise SystemExit("no trained model found; run `netanomaly train` first")
    manifest = iforest.ModelManifest(**json.loads((versions[-1] / "manifest.json").read_text(encoding="utf-8")))
    return joblib.load(versions[-1] / "model.joblib"), manifest


def cmd_generate(args: argparse.Namespace, s: Settings) -> None:
    truth = _truth_dir(s)
    injection_log = InjectionLog()
    start = date.fromisoformat(args.start)
    attack_days = {start + timedelta(days=d) for d in range(args.clean_days, args.days)}
    generate(s.paths.raw, truth, days=args.days, n_hosts=args.hosts, seed=args.seed, start=start,
             fmt=args.format, injector=make_injector(injection_log, attack_days))
    fields = list(injection_log.records[0]) if injection_log.records else ["injection_id", "attack_type"]
    with (truth / "injections.csv").open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        writer.writerows(injection_log.records)
    log.info("generated %d days (%d attack days, %d injections) -> %s; truth -> %s",
             args.days, len(attack_days), len(injection_log.records), s.paths.raw, truth)


def cmd_ingest(args: argparse.Namespace, s: Settings) -> None:
    con = connect(s.duckdb)
    for e in ingest_directory(con, s.paths.raw, load_contract(), s.paths.lake, s.duckdb.temp_directory / "stage",
                              s.ingest.max_reject_fraction):
        log.info("%-40s %-18s rows=%-8d rejected=%-6d %s", e.source_file, e.status, e.rows, e.rejected_rows, e.reason)
        if without_offset := {c: n for c, n in e.timestamps_without_offset.items() if n}:
            log.warning("%s: timestamps without UTC offset, assumed UTC: %s", e.source_file, without_offset)


def cmd_dq(args: argparse.Namespace, s: Settings) -> None:
    batch = getattr(args, "batch", None) or quality.latest_batch(s.paths.lake)
    if batch is None:
        log.warning("no ingest batch in %s; nothing to report", s.paths.lake)
        return
    report = quality.build_report(connect(s.duckdb), load_contract(), s.paths.lake, s.paths.raw, batch, s.dq)
    _, md_path = quality.write_report(report, s.paths.outputs / quality.REPORT_DIR)
    flagged = [c.name for c in report.checks if c.violations]
    volume = [f"{v.flow_date}={v.status}" for v in report.volume if v.status in ("low", "high")]
    log.info("dq batch %s: %d flows, %d rejected; checks with violations: %s; volume flags: %s -> %s",
             batch, report.flows, report.rejected_rows, flagged or "none", volume or "none", md_path)
    if report.timestamps_without_offset:
        log.warning("dq batch %s: %d timestamp values without UTC offset were assumed UTC (see report)",
                    batch, sum(w.values for w in report.timestamps_without_offset))


def cmd_features(args: argparse.Namespace, s: Settings) -> None:
    rows = features.build_host_window(connect(s.duckdb), s.paths.lake, _host_window_dir(s), s.window_minutes)
    log.info("host-window feature rows: %d", rows)


def cmd_baselines(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    baselines.require_usable_inputs(registry, load_contract(registry.contract))
    out = s.paths.features / "host_baseline"
    rows = baselines.build_host_baseline(connect(s.duckdb), s.paths.lake, out, s.duckdb.temp_directory,
                                         s.window_minutes, s.baseline)
    log.info("host-baseline rows: %d (lookback %d days) -> %s", rows, s.baseline.lookback_days, out)


def cmd_novelty(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    novelty.require_usable_inputs(registry, load_contract(registry.contract))
    out, state = s.paths.features / "host_novelty", s.paths.features / "novelty_state"
    run = novelty.build_host_novelty(connect(s.duckdb), s.paths.lake, out, state, s.window_minutes,
                                     rebuild=getattr(args, "rebuild", False))
    recomputed = f"{run.recomputed[0]}..{run.recomputed[-1]}" if run.recomputed else "none (lake unchanged)"
    log.info("host-novelty rows: %d over %d days; recomputed: %s -> %s", run.rows, run.days, recomputed, out)


def cmd_timing(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    timing.require_usable_inputs(registry, load_contract(registry.contract))
    out = s.paths.features / "host_timing"
    rows = timing.build_host_timing(connect(s.duckdb), s.paths.lake, out, s.duckdb.temp_directory,
                                    s.window_minutes, s.timing)
    log.info("host-timing rows: %d (history %d h, min %d events) -> %s", rows, s.timing.history_hours,
             s.timing.min_events, out)


def cmd_feature_cards(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    report = feature_cards.build_cards(connect(s.duckdb), registry, load_contract(registry.contract), s.paths.lake,
                                       s.paths.features, _truth_dir(s), s.window_minutes, s.feature_cards)
    _, md_path = feature_report.write_report(report, s.paths.outputs / feature_cards.REPORT_DIR)
    log.info("feature cards: %d analysed, %d not analysed, reference %s, evaluated on %s (synthetic diagnostics) -> %s",
             len(report.cards), len(report.not_analysed), ",".join(map(str, report.reference_days)) or "none",
             ",".join(map(str, report.eval_days)) or "none",
             md_path)


def cmd_train(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    contract = load_contract(registry.contract)
    con = connect(s.duckdb)
    names = iforest.model_features(registry, contract)
    split = iforest.time_split(iforest.feature_dates(con, _host_window_dir(s)), s.split.train_fraction)
    _, manifest, out = iforest.train(con, _host_window_dir(s), names, iforest.log1p_features(registry, names), split,
                                     s.model, s.paths.models, registry.registry_version)
    log.info("trained %s on %d of %d rows from %s..%s (%d days); scores only days after %s; features %s -> %s",
             manifest.model_version, manifest.train_rows, manifest.train_period_rows, split.train_dates[0],
             split.train_end, len(split.train_dates), manifest.score_after, ",".join(names), out)


def cmd_score(args: argparse.Namespace, s: Settings) -> None:
    model, manifest = _latest_model(s)
    registry = feature_registry.load_registry()
    iforest.check_scorable(manifest, registry, load_contract(registry.contract))
    n = iforest.score(connect(s.duckdb), _host_window_dir(s), model, manifest, s.paths.outputs / SCORES_FILE,
                      s.batch_rows)
    log.info("scored %d rows after %s with %s", n, manifest.score_after, manifest.model_version)


def cmd_stability(args: argparse.Namespace, s: Settings) -> None:
    registry = feature_registry.load_registry()
    con = connect(s.duckdb)
    names = iforest.model_features(registry, load_contract(registry.contract))
    split = iforest.time_split(iforest.feature_dates(con, _host_window_dir(s)), s.split.train_fraction)
    report = stability.build_report(con, _host_window_dir(s), names, iforest.log1p_features(registry, names), split,
                                    s.model, s.stability, s.alert_budget_per_day,
                                    s.duckdb.temp_directory / "stability", s.batch_rows, registry.registry_version)
    _, md_path = stability.write_report(report, s.paths.outputs / stability.REPORT_DIR)
    ss = report["seed_stability"]
    log.info("stability (label-free, %d scored rows): seed rho median %.4f, top-%d overlap median %.3f; "
             "%d curve points -> %s", report["score_rows"], ss["spearman"]["median"], report["top_k"],
             ss["topk_overlap_mean"]["median"], len(report["sample_size_curve"]), md_path)


def cmd_alerts(args: argparse.Namespace, s: Settings) -> None:
    out = s.paths.outputs / ALERTS_FILE
    n = alerts.write_top_alerts(connect(s.duckdb), s.paths.outputs / SCORES_FILE, s.paths.lake,
                                s.alert_budget_per_day, s.window_minutes, out)
    log.info("wrote %d alerts (budget %d/day) -> %s", n, s.alert_budget_per_day, out)


def cmd_evaluate(args: argparse.Namespace, s: Settings) -> None:
    con = connect(s.duckdb)
    _, manifest = _latest_model(s)
    if manifest.score_after is not None:
        by_day = labels.injected_windows_by_date(con, s.paths.lake, _truth_dir(s), s.window_minutes)
        train_days = {date.fromisoformat(d) for d in manifest.train_dates}
        after = date.fromisoformat(manifest.score_after)
        log.info("held-out check (synthetic truth, after training): injected host-windows on training days %d, "
                 "on scored days %d", sum(n for d, n in by_day.items() if d in train_days),
                 sum(n for d, n in by_day.items() if d > after))
    for k in args.k:
        for attack, found, total, best in alerts.recall_at_k(con, s.paths.outputs / SCORES_FILE, s.paths.lake,
                                                             _truth_dir(s), k, s.window_minutes):
            log.info("recall@%-4d %-16s %d/%d  best rank %s", k, attack, found, total, best)


def cmd_schema_doc(args: argparse.Namespace, s: Settings) -> None:
    out = Path(args.config).resolve().parent / "SCHEMA.md"
    out.write_text(load_contract().to_markdown(), encoding="utf-8")
    log.info("wrote %s", out)


def cmd_feature_doc(args: argparse.Namespace, s: Settings) -> None:
    out = Path(args.config).resolve().parent / "FEATURES.md"
    from netanomaly.poc.featureset import features_document

    registry = feature_registry.load_registry()
    legacy = feature_registry.to_markdown(registry, load_contract(registry.contract))
    out.write_text(features_document(legacy), encoding="utf-8")
    log.info("wrote %s", out)


def cmd_run(args: argparse.Namespace, s: Settings) -> None:
    for step in (cmd_ingest, cmd_dq, cmd_features, cmd_train, cmd_score, cmd_alerts):
        step(args, s)


# --- Parquet-only PoC (ADR-024): `netanomaly poc <stage> --config poc.yaml` ---------------------------------------

def _poc_config(args: argparse.Namespace):
    from netanomaly.poc.config import load_poc_config

    overrides = {"work_dir": args.work_dir} if getattr(args, "work_dir", None) else None
    return load_poc_config(Path(args.poc_config), overrides)


def cmd_poc(args: argparse.Namespace, s: Settings) -> None:
    from netanomaly.poc import experiment as exp
    from netanomaly.poc.config import BandSettings

    cfg = _poc_config(args)
    stage = args.poc_stage
    if stage == "profile":
        log.info("profile -> %s", exp.run_profile(cfg))
        return
    if stage == "experiment":
        log.info("report -> %s", exp.run_experiment(cfg, rebuild_features=args.rebuild_features))
        return
    ctx = exp.open_context(cfg, rebuild_features=getattr(args, "rebuild_features", False))
    log.info("experiment id %s -> %s", ctx.experiment_id, ctx.exp_dir)
    if stage == "features":
        log.info("feature table %s: %d rows, features %s -> %s", ctx.table.key, ctx.table.rows, ctx.table.features,
                 ctx.table.path)
    elif stage == "train":
        exp.train_models(ctx)
    elif stage == "score":
        exp.score_models(ctx)
        exp.robustness(ctx)
    elif stage == "report":
        bands = None
        if args.bands:
            import yaml

            bands = BandSettings.model_validate(yaml.safe_load(Path(args.bands).read_text(encoding="utf-8")))
        log.info("report -> %s", exp.finalize(ctx, bands))
    elif stage == "search":
        log.info("search -> %s", exp.run_search(ctx))


def add_poc_parser(sub: argparse._SubParsersAction) -> None:
    poc = sub.add_parser("poc", help="Parquet-only PoC: profile, features, IF/OCSVM, bands, report (ADR-024)")
    stages = poc.add_subparsers(dest="poc_stage", required=True)
    helps = {"profile": "inspect the external Parquet (files, schema, mapping, nulls, span, cardinalities)",
             "features": "build/reuse the host x window feature table",
             "train": "fit Isolation Forest / One-Class SVM on the training period",
             "score": "score every window with the trained models (+ seed/contamination robustness fits)",
             "report": "calibrate bands, write tables, diagnostics, charts and report.md",
             "experiment": "features -> train -> score -> report in one go",
             "search": "compare configured parameter candidates on the validation period"}
    for name, text in helps.items():
        p = stages.add_parser(name, help=text)
        p.add_argument("--config", dest="poc_config", required=True, help="PoC YAML config (see poc.example.yaml)")
        p.add_argument("--work-dir", help="override work_dir (must be outside the repository for real data)")
        if name in ("features", "experiment", "train", "search"):
            p.add_argument("--rebuild-features", action="store_true", help="recompute the cached feature table")
        if name == "report":
            p.add_argument("--bands", help="YAML with band settings (re-band without retraining)")
    poc.set_defaults(func=cmd_poc)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="netanomaly", description=__doc__)
    p.add_argument("--config", default="config.yaml")
    p.add_argument("--root", help="dataset root; overrides every path to ROOT/{raw,lake,features,models,outputs}")
    sub = p.add_subparsers(dest="command", required=True)
    g = sub.add_parser("generate", help="write synthetic raw data with persistent hosts and injected attacks")
    g.add_argument("--days", type=int, default=6)
    g.add_argument("--clean-days", type=int, default=3, help="leading days with no injected attacks")
    g.add_argument("--hosts", type=int, default=300)
    g.add_argument("--seed", type=int, default=7)
    g.add_argument("--start", default="2026-09-01")
    g.add_argument("--format", choices=("parquet", "csv"), default="parquet")
    g.set_defaults(func=cmd_generate)
    for name, func in (("ingest", cmd_ingest), ("features", cmd_features), ("baselines", cmd_baselines),
                       ("timing", cmd_timing),
                       ("train", cmd_train), ("score", cmd_score), ("alerts", cmd_alerts), ("run", cmd_run),
                       ("schema-doc", cmd_schema_doc), ("feature-doc", cmd_feature_doc)):
        sub.add_parser(name).set_defaults(func=func)
    n = sub.add_parser("novelty", help="new-destination / new-port rates from a persistent seen set (V2-3)")
    n.add_argument("--rebuild", action="store_true", help="discard the seen set and recompute every day")
    n.set_defaults(func=cmd_novelty)
    st = sub.add_parser("stability", help="sample-size curve and seed stability on the held-out days -> outputs/stability/ (V3)")
    st.set_defaults(func=cmd_stability)
    fc = sub.add_parser("feature-cards", help="per-feature diagnostics, synthetic AUROC -> outputs/feature_cards/ (V2-5)")
    fc.set_defaults(func=cmd_feature_cards)
    q = sub.add_parser("dq", help="data-quality report for one ingest batch -> outputs/dq/dq_<batch>.{json,md}")
    q.add_argument("--batch", help="ingest_batch_id to report on (default: newest in the ledger)")
    q.set_defaults(func=cmd_dq)
    e = sub.add_parser("evaluate", help="recall@K against injected attacks (synthetic data only)")
    e.add_argument("--k", type=int, nargs="+", default=[50, 100, 500])
    e.set_defaults(func=cmd_evaluate)
    add_poc_parser(sub)
    return p


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    args.func(args, _settings(args))


if __name__ == "__main__":
    main()
