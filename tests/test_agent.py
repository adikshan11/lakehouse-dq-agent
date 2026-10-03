import json

import duckdb
import pytest

from lakehouse.agent import ProposedTest, Warehouse, heuristic_tests, write_outputs


@pytest.fixture()
def warehouse(tmp_path):
    path = tmp_path / "warehouse.duckdb"
    con = duckdb.connect(str(path))
    con.execute(
        "create table stg_yellow_trips as select * from (values "
        "('a', 'cash', 10.0), ('b', 'credit_card', 12.5), ('c', 'cash', -1.0)) "
        "t(trip_id, payment_method, fare_amount)"
    )
    con.execute("create table fct_daily_zone_trips as select 1 as location_id, 5 as trips")
    con.execute("create table dim_zones as select 1 as location_id, 'Manhattan' as borough")
    con.close()
    return Warehouse(path)


def test_profile_reports_values_and_range(warehouse):
    profile = warehouse.profile("stg_yellow_trips", "fare_amount")
    assert profile["rows"] == 3 and profile["nulls"] == 0
    assert profile["min"] == -1.0


def test_query_rejects_writes(warehouse):
    with pytest.raises(ValueError):
        warehouse.query("delete from dim_zones")


def test_failing_proposals_become_findings(warehouse, tmp_path):
    tests = heuristic_tests(warehouse) + [
        ProposedTest(model="stg_yellow_trips", column="fare_amount", test="accepted_range", min_value=0)
    ]
    summary = write_outputs(warehouse, tests, tmp_path, tmp_path / "report.json", "heuristic")

    assert summary["failing"] == [
        {
            "model": "stg_yellow_trips",
            "column": "fare_amount",
            "test": "accepted_range",
            "failed_rows": 1,
            "reason": "",
        }
    ]
    written = sorted(p.name for p in (tmp_path / "tests" / "agent_generated").glob("*.sql"))
    assert "agent__stg_yellow_trips__trip_id__unique.sql" in written
    assert "agent__stg_yellow_trips__fare_amount__accepted_range.sql" not in written
    unique_sql = (
        tmp_path / "tests" / "agent_generated" / "agent__stg_yellow_trips__trip_id__unique.sql"
    ).read_text()
    assert "{{ ref('stg_yellow_trips') }}" in unique_sql
    assert json.loads((tmp_path / "report.json").read_text())["proposed"] == len(tests)
