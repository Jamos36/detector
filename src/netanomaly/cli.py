"""Command line: `uv run netanomaly [command] [--config config.yaml]`.

    uv run netanomaly              same as `run`: profile -> features -> train -> score -> report
    uv run netanomaly profile      only inspect the input Parquet (columns, mapping, row counts, dates)
    uv run netanomaly features     build the host x window feature table
    uv run netanomaly train        fit Isolation Forest and One-Class SVM on the training period
    uv run netanomaly score        score every window (plus seed / contamination robustness fits)
    uv run netanomaly report       bands, tables, charts, report.html (add --bands FILE to re-band, no retraining)
    uv run netanomaly search       compare the parameter candidates in config.yaml on the validation period
    uv run netanomaly docs         regenerate FEATURES.md and SCHEMA.md
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

log = logging.getLogger("netanomaly")
COMMANDS = {
    "run": "profile + features + train + score + report (the default)",
    "profile": "inspect the input Parquet: columns, field mapping, nulls, date span, row counts",
    "features": "build (or reuse) the host x window feature table",
    "train": "fit Isolation Forest and One-Class SVM on the training period",
    "score": "score every window with the trained models (+ seed/contamination robustness fits)",
    "report": "calibrate bands, write tables, diagnostics, charts, report.md and report.html",
    "search": "compare the parameter candidates listed under `search:` on the validation period",
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
    return p


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
    if args.command == "run":
        log.info("profile -> %s", exp.run_profile(cfg))
        return exp.run_experiment(cfg, rebuild_features=args.rebuild_features)
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
        print(f"\nDone. {'Open this file in your browser: ' + str(html) if html and html.exists() else out}")


if __name__ == "__main__":
    main()
