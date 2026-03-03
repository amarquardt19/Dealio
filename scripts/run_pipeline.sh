#!/bin/bash
# Run the full pipeline: dbt build → sync mart to Azure SQL + GitHub
# Usage: ./scripts/run_pipeline.sh

set -e
cd "$(dirname "$0")/.."

echo "═══ Running dbt build ═══"
dbt build

echo ""
echo "═══ Syncing mart → Azure SQL + GitHub ═══"
python3 scripts/sync_mart.py

echo ""
echo "═══ Pipeline complete. CSV is live on GitHub. ═══"
