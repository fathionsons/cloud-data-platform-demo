PYTHON ?= python
DBT_PROFILES_DIR ?= dbt
DBT_PROJECT_DIR ?= dbt

.PHONY: setup ingest-local ingest-aws dbt-run dbt-test dashboard lint format test

setup:
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements-dev.txt
	@if [ ! -f dbt/profiles.yml ]; then cp dbt/profiles.yml.example dbt/profiles.yml; fi
	@mkdir -p data/bronze data/silver data/gold

ingest-local:
	$(PYTHON) -m pipeline.ingest --mode local

ingest-aws:
	$(PYTHON) -m pipeline.ingest --mode aws

dbt-run:
	dbt deps --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR)
	dbt run --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --target duckdb

dbt-test:
	dbt test --project-dir $(DBT_PROJECT_DIR) --profiles-dir $(DBT_PROFILES_DIR) --target duckdb

dashboard:
	streamlit run dashboard/app.py

lint:
	ruff check .
	black --check .

format:
	black .

test:
	pytest
