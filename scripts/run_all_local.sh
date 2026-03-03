#!/usr/bin/env bash
set -euo pipefail

./scripts/bootstrap_local.sh
make ingest-local
make dbt-run
make dbt-test

echo "Pipeline and dbt steps completed."
echo "Start dashboard with: make dashboard"
