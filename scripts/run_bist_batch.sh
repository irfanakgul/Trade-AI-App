#!/bin/zsh
set -euo pipefail

cd "$(dirname "$0")/.."

echo "[BIST] batch start: $(date '+%Y-%m-%d %H:%M:%S')"

.venv/bin/python -u main_bist_data.py
.venv/bin/python -u main_bist_indicators.py

echo "[BIST] batch end:   $(date '+%Y-%m-%d %H:%M:%S')"