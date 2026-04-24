@echo off
cd /d "%~dp0"
set UV_CACHE_DIR=.uv-cache
echo Starting medical device traceability system...
echo Open http://127.0.0.1:8000 after the server starts.
uv run python -m app.server
