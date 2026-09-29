@echo off
rem Double-click (or run from a terminal) to install everything and run the whole pipeline with config.yaml.
rem Needs uv: https://docs.astral.sh/uv/getting-started/installation/
cd /d "%~dp0"
where uv >nul 2>nul || (echo uv is not installed. Install it from https://docs.astral.sh/uv/ and run this again. & pause & exit /b 1)
uv sync || (pause & exit /b 1)
uv run netanomaly %*
pause
