VENV?=.venv
PYTHON?=python3
PIP?=$(PYTHON) -m pip

install:
	$(PIP) install -r requirements.txt

deps:
	cd dbt && DBT_PROFILES_DIR=. dbt deps

seed:
	cd dbt && DBT_PROFILES_DIR=. dbt seed

run:
	cd dbt && DBT_PROFILES_DIR=. dbt run

test:
	cd dbt && DBT_PROFILES_DIR=. dbt test

all: deps seed run test

lint:
	sqlfluff lint dbt/models dbt/tests --config .sqlfluff

fmt:
	sqlfluff fix dbt/models dbt/tests --config .sqlfluff

docs:
	cd dbt && DBT_PROFILES_DIR=. dbt docs generate

.PHONY: install deps seed run test all lint fmt docs
