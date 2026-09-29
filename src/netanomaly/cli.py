"""Command line: `uv run netanomaly [command] [--config config.yaml]`.

A. Historical year: train on `split.train`, calibrate on `split.validation`, test once on `split.test`
    uv run netanomaly              same as `run`: development (profile -> features -> train -> score -> report,
                                   saves the model bundle) and then the final test with the frozen bundle
    uv run netanomaly profile      only inspect the input Parquet (columns, mapping, row counts, dates)
    uv run netanomaly features     build the host x window feature table
    uv run netanomaly train        fit Isolation Forest and One-Class SVM on the training period
    uv run netanomaly score        score the training + validation windows (plus seed / contamination fits)
    uv run netanomaly report       bands, tables, charts, development report, model bundle
                                   (add --bands FILE to re-band without retraining: a new bundle id)
    uv run netanomaly search       compare the parameter candidates in config.yaml on the validation period
    uv run netanomaly test         final test: score the reserved test period once with the frozen bundle
                                   (default: this config's bundle; or --bundle DIR)
B. New data
    uv run netanomaly score-new --bundle DIR --input PATH [PATH ...] [--history DIR]
                                   score new Parquet files with a saved bundle (no fitting, no tuning)
Other
    uv run netanomaly docs         regenerate FEATURES.md and SCHEMA.md
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

log = logging.getLogger("netanomaly")
COMMANDS = {
    "run": "development (train + validation) + final test with the frozen bundle (the default)",
    "profile": "inspect the input Parquet: columns, field mapping, nulls, date span, row counts",
    "features": "build (or reuse) the host x window feature table",
    "train": "fit Isolation Forest and One-Class SVM on the training period",
    "score": "score the training + validation windows (+ seed/contamination robustness fits)",
    "report": "calibrate bands on validation, write tables, charts, development report and the model bundle",
    "search": "compare the parameter candidates listed under `search:` on the validation period",
    "test": "final test: score the reserved test period once with the frozen bundle",
    "score-new": "score new Parquet files (--input) with a saved model bundle (--bundle)",
    "docs": "regenerate FEATURES.md and SCHEMA.md from the code",
}


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="netanomaly", description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument("command", nargs="?", default="run", choices=list(COMMANDS),
                   help="\n".join(f"{k:9} {v}" for k, v in COMMANDS.items()))
    p.add_argument("--config", default="config.yaml", help="config file (default: config.yaml)")
    p.add_argument("--work-dir", help="override work_dir (where outputs go)")
    p.add_argument("--bands", help="report only: YAML with band settings (re-band without retraining)")
    p.add_argument("--rebuild-features", action="store_true", help="recompute the cached feature table")
    p.add_argument("--bundle", help="test / score-new: model bundle folder (<work_dir>/bundles/<bundle_id>)")
    p.add_argument("--input", nargs="+", help="score-new: new Parquet file(s), folder(s) or glob(s)")
    p.add_argument("--history", help="score-new: continue the relationship history from this earlier scoring run "
                                     "(default: the bundle's own history)")
    return p


def _scoring(args: argparse.Namespace, cfg) -> Path:
    from netanomaly import scoring

    if args.command == "test":
        return scoring.run_holdout(cfg, Path(args.bundle) if args.bundle else scoring.bundle_for_config(cfg))
    if not args.bundle or not args.input:
        raise ValueError("score-new needs --bundle DIR and --input PATH [PATH ...]")
    return scoring.score_new(cfg, Path(args.bundle), args.input, Path(args.history) if args.history else None)


def _docs() -> None:
    from netanomaly.featureset import features_document
    from netanomaly.schema import load_contract

    root = Path(__file__).resolve().parents[2]
    (root / "FEATURES.md").write_text(features_document(), encoding="utf-8")
    (root / "SCHEMA.md").write_text(load_contract().to_markdown(), encoding="utf-8")
    log.info("wrote %s and %s", root / "FEATURES.md", root / "SCHEMA.md")


def dispatch(args: argparse.Namespace) -> Path | None:
    from netanomaly import experiment as exp
    from netanomaly.config import BandSettings, load_poc_config

    if args.command == "docs":
        _docs()
        return None
    cfg = load_poc_config(Path(args.config), {"work_dir": args.work_dir} if args.work_dir else None)
    if args.command == "profile":
        return exp.run_profile(cfg)
    if args.command in ("test", "score-new"):
        return _scoring(args, cfg)
    if args.command == "run":
        from netanomaly import scoring

        log.info("profile -> %s", exp.run_profile(cfg))
        dev = exp.run_experiment(cfg, rebuild_features=args.rebuild_features)
        if cfg.split.test is None and cfg.split.fractions[2] == 0:
            return dev
        print(f"\nDevelopment report (training + validation): {dev.with_suffix('.html')}")
        return scoring.run_holdout(cfg, scoring.bundle_for_config(cfg))
    ctx = exp.open_context(cfg, rebuild_features=args.rebuild_features)
    log.info("experiment %s -> %s", ctx.experiment_id, ctx.exp_dir)
    if args.command == "features":
        log.info("feature table: %d rows, features %s -> %s", ctx.table.rows, ctx.table.features, ctx.table.path)
        return ctx.table.path
    if args.command == "train":
        exp.train_models(ctx)
        return ctx.exp_dir / "models"
    if args.command == "score":
        exp.score_models(ctx)
        exp.robustness(ctx)
        return ctx.exp_dir / exp.SCORES_RAW
    if args.command == "report":
        import yaml

        bands = (BandSettings.model_validate(yaml.safe_load(Path(args.bands).read_text(encoding="utf-8")))
                 if args.bands else None)
        return exp.finalize(ctx, bands)
    return exp.run_search(ctx)  # search


def main(argv: list[str] | None = None) -> None:
    for stream in (sys.stdout, sys.stderr):  # Windows consoles default to cp1252
        if hasattr(stream, "reconfigure"):
            stream.reconfigure(encoding="utf-8", errors="replace")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    try:
        out = dispatch(args)
    except (FileNotFoundError, ValueError) as exc:  # config, data and boundary errors: a readable message
        log.error("%s", exc)
        raise SystemExit(1) from exc
    if out is not None:
        html = out.with_suffix(".html") if out.suffix == ".md" else None
        label = {"run": "Final test report", "test": "Final test report", "score-new": "New-data report"}.get(
            args.command, "Open this file in your browser")
        print(f"\nDone. {label + ': ' + str(html) if html and html.exists() else out}")


if __name__ == "__main__":
    main()
