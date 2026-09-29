#!/usr/bin/env sh
# Install everything and run the whole pipeline with config.yaml:   ./run.sh
# Extra arguments are passed on, e.g.  ./run.sh profile   or   ./run.sh --config other.yaml
# Needs uv: https://docs.astral.sh/uv/getting-started/installation/
set -e
cd "$(dirname "$0")"
command -v uv >/dev/null 2>&1 || { echo "uv is not installed. Install it from https://docs.astral.sh/uv/ and run this again."; exit 1; }
uv sync
uv run netanomaly "$@"
