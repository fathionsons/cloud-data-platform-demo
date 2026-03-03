#!/usr/bin/env bash
set -euo pipefail

python -m pip install --upgrade pip
python -m pip install -r requirements-dev.txt

if [[ ! -f dbt/profiles.yml ]]; then
  cp dbt/profiles.yml.example dbt/profiles.yml
fi

mkdir -p data/bronze data/silver data/gold

echo "Local bootstrap complete."
