import argparse
import json
import os
import re
from pathlib import Path
from typing import Literal

import duckdb
from pydantic import BaseModel, Field, ValidationError

from lakehouse.config import load_paths

MODEL = os.environ.get("DQ_AGENT_MODEL", "claude-opus-5-5")
TABLES = ["stg_yellow_trips", "fct_daily_zone_trips", "dim_zones"]
READ_ONLY = re.compile(r"^\s*(select|with)\b", re.IGNORECASE)


class ProposedTest(BaseModel):
    model: str
    column: str
    test: Literal["not_null", "unique", "accepted_values", "accepted_range"]
    values: list[str] | None = None
    min_value: float | None = None
    max_value: float | None = None
    reason: str = Field(default="", max_length=300)


class Warehouse:
    def __init__(self, path: Path):
        self.con = duckdb.connect(str(path), read_only=True)

    def columns(self, table: str) -> list[tuple[str, str]]:
        return self.con.execute(
            "select column_name, data_type from information_schema.columns "
            "where table_name = ? order by ordinal_position",
            [table],
        ).fetchall()

    def profile(self, table: str, column: str) -> dict:
        if column not in {name for name, _ in self.columns(table)}:
            raise ValueError(f"unknown column {table}.{column}")
        col = f'"{column}"'
        rows, nulls, distinct = self.con.execute(
            f"select count(*), count(*) - count({col}), count(distinct {col}) from {table}"
        ).fetchone()
        result = {"table": table, "column": column, "rows": rows, "nulls": nulls, "distinct": distinct}
        numeric = self.con.execute(
            f"select try_cast(min({col}) as double), try_cast(max({col}) as double),"
            f" try_cast(quantile_cont(try_cast({col} as double), 0.001) as double),"
            f" try_cast(quantile_cont(try_cast({col} as double), 0.999) as double) from {table}"
        ).fetchone()
        if numeric[0] is not None:
            result.update(min=numeric[0], max=numeric[1], p001=numeric[2], p999=numeric[3])
        if distinct <= 20:
            result["values"] = [
                {"value": str(v), "count": c}
                for v, c in self.con.execute(
                    f"select {col}, count(*) from {table} group by 1 order by 2 desc"
                ).fetchall()
            ]
        return result

    def query(self, sql: str, limit: int = 50) -> list[tuple]:
        if not READ_ONLY.match(sql) or ";" in sql.strip().rstrip(";"):
            raise ValueError("only a single SELECT or WITH query is allowed")
        return self.con.execute(f"select * from ({sql.rstrip(';')}) limit {limit}").fetchall()

    def failures(self, test: ProposedTest) -> int:
        return self.con.execute(f"select count(*) from ({failing_rows_sql(test, test.model)})").fetchone()[0]


def failing_rows_sql(test: ProposedTest, relation: str) -> str:
    col = f'"{test.column}"'
    if test.test == "not_null":
        return f"select * from {relation} where {col} is null"
    if test.test == "unique":
        return f"select {col}, count(*) as n from {relation} group by 1 having count(*) > 1"
    if test.test == "accepted_values":
        allowed = ", ".join("'" + v.replace("'", "''") + "'" for v in test.values or [])
        return f"select * from {relation} where cast({col} as varchar) not in ({allowed})"
    bounds = []
    if test.min_value is not None:
        bounds.append(f"{col} < {test.min_value}")
    if test.max_value is not None:
        bounds.append(f"{col} > {test.max_value}")
    return f"select * from {relation} where {' or '.join(bounds) or 'false'}"


def heuristic_tests(wh: Warehouse) -> list[ProposedTest]:
    tests = []
    for table in TABLES:
        for column, dtype in wh.columns(table):
            p = wh.profile(table, column)
            if p["nulls"] == 0:
                tests.append(
                    ProposedTest(model=table, column=column, test="not_null", reason="no nulls observed")
                )
            if p["distinct"] == p["rows"] and p["rows"] > 0:
                tests.append(
                    ProposedTest(model=table, column=column, test="unique", reason="all values distinct")
                )
            if "values" in p and p["distinct"] < p["rows"] and dtype.upper() in {"VARCHAR", "TEXT"}:
                tests.append(
                    ProposedTest(
                        model=table,
                        column=column,
                        test="accepted_values",
                        values=[v["value"] for v in p["values"]],
                        reason="low cardinality",
                    )
                )
            if (
                "min" in p
                and p["min"] is not None
                and p["min"] >= 0
                and dtype.upper() not in {"DATE", "TIMESTAMP"}
            ):
                tests.append(
                    ProposedTest(
                        model=table,
                        column=column,
                        test="accepted_range",
                        min_value=0,
                        reason="never negative",
                    )
                )
    return tests


def claude_tests(wh: Warehouse) -> list[ProposedTest]:
    import anthropic
    from anthropic import beta_tool

    proposals: list[ProposedTest] = []

    @beta_tool
    def list_columns(table: str) -> str:
        """List the columns and types of a warehouse table.

        Args:
            table: One of stg_yellow_trips, fct_daily_zone_trips, dim_zones.
        """
        return json.dumps(wh.columns(table))

    @beta_tool
    def profile_column(table: str, column: str) -> str:
        """Return row count, null count, distinct count, min/max, 0.1 and 99.9 percentiles and top values.

        Args:
            table: Table name.
            column: Column name.
        """
        return json.dumps(wh.profile(table, column), default=str)

    @beta_tool
    def run_query(sql: str) -> str:
        """Run one read-only SELECT against the DuckDB warehouse and return up to 50 rows.

        Args:
            sql: A single SELECT or WITH statement.
        """
        return json.dumps(wh.query(sql), default=str)

    @beta_tool
    def submit_tests(tests_json: str) -> str:
        """Submit the final list of proposed dbt tests as a JSON array.

        Args:
            tests_json: JSON array of objects with keys model, column, test (not_null, unique,
                accepted_values or accepted_range), optional values, min_value, max_value, and reason.
        """
        try:
            items = [ProposedTest(**item) for item in json.loads(tests_json)]
        except (json.JSONDecodeError, TypeError, ValidationError) as error:
            return f"rejected: {error}"
        proposals.extend(items)
        return f"accepted {len(items)} tests"

    prompt = (
        "You are a data quality engineer. Profile the DuckDB tables "
        f"{', '.join(TABLES)} with the tools, then propose dbt tests that encode business rules for NYC "
        "taxi trips (for example fares and durations that should never be negative, valid payment methods, "
        "keys that must be unique). Prefer tests that would catch real pipeline regressions "
        "over trivial ones. "
        "Use profile_column and run_query to check each rule against the data before proposing it. "
        "When done, call submit_tests exactly once with all proposals."
    )
    client = anthropic.Anthropic()
    runner = client.beta.messages.tool_runner(
        model=MODEL,
        max_tokens=16000,
        output_config={"effort": "high"},
        tools=[list_columns, profile_column, run_query, submit_tests],
        messages=[{"role": "user", "content": prompt}],
    )
    for message in runner:
        if message.stop_reason == "refusal":
            raise RuntimeError("model declined the request")
    return proposals


def write_outputs(
    wh: Warehouse, tests: list[ProposedTest], dbt_dir: Path, report_path: Path, source: str
) -> dict:
    checked = [(t, wh.failures(t)) for t in tests]
    passing = [t for t, failed in checked if failed == 0]
    failing = [(t, failed) for t, failed in checked if failed > 0]

    out_dir = dbt_dir / "tests" / "agent_generated"
    out_dir.mkdir(parents=True, exist_ok=True)
    for old in out_dir.glob("*.sql"):
        old.unlink()
    for t in passing:
        name = f"agent__{t.model}__{t.column}__{t.test}.sql"
        header = f"-- Generated by lakehouse.agent ({source}): {t.reason or t.test}\n"
        (out_dir / name).write_text(header + failing_rows_sql(t, "{{ ref('" + t.model + "') }}") + "\n")
    summary = {
        "source": source,
        "proposed": len(tests),
        "passing": len(passing),
        "failing": [
            {"model": t.model, "column": t.column, "test": t.test, "failed_rows": failed, "reason": t.reason}
            for t, failed in failing
        ],
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(summary, indent=2))
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Profile the warehouse and propose dbt tests")
    parser.add_argument("--offline", action="store_true", help="Use rule-based proposals instead of Claude")
    args = parser.parse_args()

    paths = load_paths()
    wh = Warehouse(paths.root / "warehouse.duckdb")
    use_claude = not args.offline and bool(os.environ.get("ANTHROPIC_API_KEY"))
    tests = claude_tests(wh) if use_claude else heuristic_tests(wh)
    dbt_dir = Path(__file__).resolve().parents[2] / "dbt"
    summary = write_outputs(
        wh, tests, dbt_dir, paths.runs / "agent_report.json", MODEL if use_claude else "heuristic"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
