#!/usr/bin/env bash
# Download the Kaggle "NFL 1st and Future" competition data.
# Usage: ./01_download_kaggle.sh [output_dir]
set -euo pipefail

OUT_DIR="${1:-$HOME/nfl_data/raw/kaggle}"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

COMPETITION="nfl-playing-surface-analytics"
echo "Downloading $COMPETITION to $OUT_DIR ..."
kaggle competitions download -c "$COMPETITION" -p "$OUT_DIR"
