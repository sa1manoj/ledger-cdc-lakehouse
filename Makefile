# Ledger: common tasks. Run `make help` for a list.
SHELL := /bin/bash
export LAKE_ROOT ?= $(CURDIR)/lake
export DUCKDB_PATH ?= $(CURDIR)/ledger.duckdb
PY := python

.PHONY: help install up down connector simulate bronze bronze-stream silver reconcile gold promote test fixture dbt-fixture benchmark clean

help:            ## show targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-14s %s\n", $$1, $$2}'

install:         ## install the package with Spark, dbt and dev extras
	$(PY) -m pip install -e ".[spark,dbt,dev]"

up:              ## start Postgres, Kafka and Kafka Connect (Debezium)
	docker compose up -d --wait

down:            ## stop containers and delete their data
	docker compose down -v

connector:       ## register the Debezium Postgres connector
	./scripts/register_connector.sh

simulate:        ## generate OLTP activity (inserts, updates, deletes, backdated fixes)
	$(PY) -m ledger.simulate --customers 200 --ticks 30 --orders-per-tick 100 --sleep 1

bronze:          ## land everything currently in Kafka into bronze, then stop
	$(PY) -m ledger.jobs.bronze --once

bronze-stream:   ## run bronze continuously (Ctrl+C to stop)
	$(PY) -m ledger.jobs.bronze

silver:          ## apply new bronze events to the SCD2 silver tables
	$(PY) -m ledger.jobs.silver

reconcile:       ## gate: compare Postgres current state with silver
	$(PY) -m ledger.reconcile

gold:            ## build and test the dbt gold layer
	cd dbt && dbt build --profiles-dir .

promote: silver reconcile gold   ## silver -> reconciliation gate -> gold (stops if the gate fails)

test:            ## unit tests (Spark tests are skipped if pyspark is not installed)
	$(PY) -m pytest -q

fixture:         ## write a synthetic silver lake to ./lake_fixture (no Docker needed)
	$(PY) -m ledger.fixtures --out $(CURDIR)/lake_fixture

dbt-fixture: fixture   ## build + test dbt against the synthetic lake
	cd dbt && LAKE_ROOT=$(CURDIR)/lake_fixture DUCKDB_PATH=$(CURDIR)/fixture.duckdb dbt build --profiles-dir .

benchmark:       ## time the daily-revenue query on Postgres vs. the gold table
	$(PY) scripts/benchmark.py

clean:           ## remove local lake, checkpoints and dbt artifacts
	rm -rf lake lake_fixture *.duckdb dbt/target dbt/logs
