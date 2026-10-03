# lakehouse-dq-agent

A small lakehouse for NYC yellow taxi trips: PySpark loads raw files into Delta Lake bronze and silver tables, quality gates stop bad loads, dbt builds the marts, and a column profiler proposes dbt tests from the data itself (rule-based by default, with an optional Claude tool-use mode).

```
TLC parquet ──► bronze (Delta, per-month replaceWhere) ──► silver (Delta MERGE, deduped)
                                   │                               │
                                   └──► quarantine (reject reason)  ├──► 7 quality checks (fail the run)
                                                                    └──► dbt on DuckDB: stg ─► dim_zones, fct_daily_zone_trips
                                                                                         └──► DQ agent ─► generated dbt tests
```

## Results (Jan-Mar 2024, one laptop, 14 cores)

| Stage | Result |
| --- | --- |
| Bronze | 9,554,778 rows across 3 months, idempotent per-month overwrite |
| Silver | 9,179,149 rows merged on a SHA-256 trip key; 1 duplicate removed |
| Quarantine | 375,628 rows (3.93%): distance 140,903, negative amount 119,432, duration 115,233, outside month 56, passenger count 4 |
| Quality gate | 7 of 7 checks passed (unique non-null keys, no negative totals, all 91 days present, reject rate under 5%) |
| Runtime | 3 min 39 s end to end (bronze 51 s, silver 144 s, checks 22 s) |
| dbt | 1 seed, 3 models, 63 data tests passing (10 hand-written, 53 proposed by the rule-based profiler and verified on the data) |

## How it works

- **Bronze** (`src/lakehouse/ingest.py`): downloads a month, normalises column names and types, and overwrites only that month's partition, so reruns never duplicate data.
- **Silver** (`src/lakehouse/transform.py`): builds a deterministic `trip_id`, tags every row that breaks a business rule with a reject reason, sends those rows to a quarantine table, removes duplicates and `MERGE`s the rest into a Delta table partitioned by pickup date.
- **Quality gate** (`src/lakehouse/quality.py`): seven checks on the loaded months; the run exits non-zero if any fails, so orchestration stops before marts are built.
- **Marts** (`dbt/`): a staging view over the silver Delta table (DuckDB `delta_scan`), a zone dimension from the TLC lookup, and a daily trips and revenue fact per pickup zone.
- **Test generator** (`src/lakehouse/agent.py`): profiles every warehouse column through read-only queries and proposes dbt tests from rules (no nulls seen, all values distinct, low-cardinality values, never negative). An optional Claude mode (`ANTHROPIC_API_KEY` set, tool-use loop with `list_columns`, `profile_column`, `run_query`, `submit_tests`) proposes business-rule tests instead; the results above come from the rule-based mode. Every proposal is executed against the data first: passing ones become dbt tests in `dbt/tests/agent_generated/`, failing ones are reported as data findings in `runs/agent_report.json`.
- **Orchestration**: `orchestration/dags/nyc_taxi_lakehouse.py` runs load, dbt, agent and regression tests monthly in Airflow. CI (`.github/workflows/ci.yml`) lints, runs unit tests and runs the whole pipeline on one month.

## Run it

Needs Python 3.10+ and Java 17.

```bash
pip install -e ".[dev]"
export LAKEHOUSE_DATA=$PWD/data
lakehouse 2024-01 2024-02 2024-03          # bronze, silver, quality gate
cd dbt && dbt build --profiles-dir . && cd ..
python -m lakehouse.agent                  # add --offline to skip Claude
cd dbt && dbt test --profiles-dir .
```

Behind a proxy that blocks Spark's Maven download, put the Delta jars on disk and set `DELTA_JARS=/path/delta-spark_2.12-3.2.1.jar,/path/delta-storage-3.2.1.jar`.

## Tests

`pytest` covers the reject rules, trip-key dedupe, the quality checks catching bad data, the agent's read-only query guard, and failing proposals being reported instead of written as tests.

Data: [NYC TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page).
