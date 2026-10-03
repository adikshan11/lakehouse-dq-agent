from dataclasses import asdict, dataclass

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


@dataclass
class CheckResult:
    name: str
    passed: bool
    observed: float
    threshold: str


def run_checks(
    silver: DataFrame, months: list[str], input_rows: int, rejected_rows: int, max_reject_rate: float = 0.05
) -> list[CheckResult]:
    scoped = silver.where(F.col("ingest_month").isin(months))
    stats = scoped.agg(
        F.count("*").alias("rows"),
        F.countDistinct("trip_id").alias("distinct_ids"),
        F.sum(F.col("trip_id").isNull().cast("int")).alias("null_ids"),
        F.sum((F.col("total_amount") < 0).cast("int")).alias("negative_totals"),
        F.sum((F.date_format("pickup_ts", "yyyy-MM") != F.col("ingest_month")).cast("int")).alias(
            "out_of_month"
        ),
        F.countDistinct("pickup_date").alias("days"),
    ).first()

    rows = stats["rows"] or 0
    reject_rate = rejected_rows / input_rows if input_rows else 1.0
    expected_days = sum(_days_in_month(m) for m in months)
    return [
        CheckResult("row_count_positive", rows > 0, rows, "> 0"),
        CheckResult("trip_id_not_null", stats["null_ids"] == 0, stats["null_ids"] or 0, "= 0"),
        CheckResult(
            "trip_id_unique",
            stats["distinct_ids"] == rows,
            rows - (stats["distinct_ids"] or 0),
            "= 0 duplicates",
        ),
        CheckResult(
            "total_amount_non_negative", stats["negative_totals"] == 0, stats["negative_totals"] or 0, "= 0"
        ),
        CheckResult("pickup_inside_month", stats["out_of_month"] == 0, stats["out_of_month"] or 0, "= 0"),
        CheckResult(
            "every_day_present", stats["days"] == expected_days, stats["days"], f"= {expected_days} days"
        ),
        CheckResult(
            "reject_rate", reject_rate <= max_reject_rate, round(reject_rate, 4), f"<= {max_reject_rate}"
        ),
    ]


def summarize(results: list[CheckResult]) -> dict:
    return {
        "passed": all(r.passed for r in results),
        "checks": [asdict(r) for r in results],
    }


def _days_in_month(month: str) -> int:
    import calendar

    year, mon = (int(part) for part in month.split("-"))
    return calendar.monthrange(year, mon)[1]
