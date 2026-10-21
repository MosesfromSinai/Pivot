#!/usr/bin/env bash
# Download the Kaggle "NFL 1st and Future" competition data.
# Usage: ./01_download_kaggle.sh [output_dir]
set -euo pipefail

OUT_DIR="${1:-$HOME/nfl_data/raw/kaggle}"
mkdir -p "$OUT_DIR"
cd "$OUT_DIR"

START=$(date +%s)

COMPETITION="nfl-playing-surface-analytics"
echo "Downloading $COMPETITION to $OUT_DIR ..."
kaggle competitions download -c "$COMPETITION" -p "$OUT_DIR"

ZIP="$COMPETITION.zip"
echo "Unzipping $ZIP ..."
if command -v unzip >/dev/null 2>&1; then
    unzip -o "$ZIP"
else
    python -m zipfile -e "$ZIP" .
fi
rm "$ZIP"

END=$(date +%s)
echo "Total download time: $((END - START)) seconds" | tee download_time.txt

ls -lh *.csv | tee file_sizes.txt
wc -l *.csv | tee line_counts.txt
