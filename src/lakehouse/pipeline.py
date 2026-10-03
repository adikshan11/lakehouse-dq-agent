import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import functions as F

from lakehouse.config import load_paths
from lakehouse.ingest import download_zones, ingest_month
from lakehouse.quality import run_checks, summarize
from lakehouse.spark import build_spark
from lakehouse.transform import build_silver


def run(months: list[str], max_reject_rate: float) -> dict:
    paths = load_paths()
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    timings: dict[str, float] = {}

    start = time.perf_counter()
    spark = build_spark()
    timings["spark_start_s"] = round(time.perf_counter() - start, 1)

    stage = time.perf_counter()
    bronze_rows = {month: ingest_month(spark, paths, month) for month in months}
    download_zones(Path(__file__).resolve().parents[2] / "dbt" / "seeds" / "taxi_zone_lookup.csv")
    timings["bronze_s"] = round(time.perf_counter() - stage, 1)

    stage = time.perf_counter()
    result = build_silver(spark, paths, months)
    timings["silver_s"] = round(time.perf_counter() - stage, 1)

    stage = time.perf_counter()
    silver = spark.read.format("delta").load(str(paths.silver))
    checks = summarize(run_checks(silver, months, result.input_rows, result.rejected_rows, max_reject_rate))
    timings["quality_s"] = round(time.perf_counter() - stage, 1)
    timings["total_s"] = round(time.perf_counter() - start, 1)

    rejects = (
        spark.read.format("delta")
        .load(str(paths.quarantine))
        .where(F.col("ingest_month").isin(months))
        .groupBy("reject_reason")
        .count()
        .orderBy(F.desc("count"))
        .collect()
    )
    report = {
        "run_id": run_id,
        "months": months,
        "cores": spark.sparkContext.defaultParallelism,
        "bronze_rows": bronze_rows,
        "silver": {
            "input_rows": result.input_rows,
            "rejected_rows": result.rejected_rows,
            "reject_rate": round(result.rejected_rows / result.input_rows, 4) if result.input_rows else None,
            "duplicate_rows": result.duplicate_rows,
            "merged_rows": result.merged_rows,
        },
        "rejects_by_reason": {row["reject_reason"]: row["count"] for row in rejects},
        "quality": checks,
        "timings": timings,
        "rows_per_second": round(result.input_rows / timings["silver_s"]) if timings["silver_s"] else None,
    }
    paths.runs.mkdir(parents=True, exist_ok=True)
    (paths.runs / f"run_{run_id}.json").write_text(json.dumps(report, indent=2))
    spark.stop()
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Load NYC yellow taxi months into the lakehouse")
    parser.add_argument("months", nargs="+", help="Months as YYYY-MM, e.g. 2024-01 2024-02")
    parser.add_argument(
        "--max-reject-rate", type=float, default=float(os.environ.get("MAX_REJECT_RATE", 0.05))
    )
    args = parser.parse_args()

    report = run(args.months, args.max_reject_rate)
    print(json.dumps(report, indent=2))
    if not report["quality"]["passed"]:
        sys.exit(1)


if __name__ == "__main__":
    main()
