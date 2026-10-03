MONTHS ?= 2024-01 2024-02 2024-03
export LAKEHOUSE_DATA ?= $(CURDIR)/data

.PHONY: install lint test pipeline dbt agent all

install:
	pip install -e ".[dev]"

lint:
	ruff check src tests orchestration

test:
	pytest -q

pipeline:
	lakehouse $(MONTHS)

dbt:
	cd dbt && dbt build --profiles-dir .

agent:
	python -m lakehouse.agent
	cd dbt && dbt test --profiles-dir .

all: lint test pipeline dbt agent
