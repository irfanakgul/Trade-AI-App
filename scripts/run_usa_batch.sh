#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[USA] batch start: $(date '+%Y-%m-%d %H:%M:%S')"

venv/bin/python3 -u main_usa_data.py
venv/bin/python3 -u main_usa_indicators.py

echo "[USA] batch end:   $(date '+%Y-%m-%d %H:%M:%S')"