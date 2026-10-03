from datetime import datetime

from lakehouse.ingest import BRONZE_COLUMNS
from lakehouse.quality import run_checks
from lakehouse.transform import conform, deduplicate, with_reject_reason


def bronze_row(**overrides):
    row = {name: None for name in BRONZE_COLUMNS}
    row.update(
        vendorid=1,
        tpep_pickup_datetime=datetime(2024, 1, 10, 8, 0),
        tpep_dropoff_datetime=datetime(2024, 1, 10, 8, 20),
        passenger_count=1,
        trip_distance=3.2,
        pulocationid=161,
        dolocationid=237,
        payment_type=1,
        fare_amount=18.0,
        tip_amount=4.0,
        total_amount=26.5,
        ingest_month="2024-01",
    )
    row.update(overrides)
    return row


def make_bronze(spark, rows):
    schema = ", ".join(f"{name} {dtype}" for name, dtype in BRONZE_COLUMNS.items()) + ", ingest_month string"
    return spark.createDataFrame([tuple(r.values()) for r in rows], schema)


def test_reject_reasons(spark):
    rows = [
        bronze_row(),
        bronze_row(
            tpep_pickup_datetime=datetime(2023, 12, 31, 23, 0),
            tpep_dropoff_datetime=datetime(2023, 12, 31, 23, 30),
        ),
        bronze_row(tpep_dropoff_datetime=datetime(2024, 1, 10, 8, 0)),
        bronze_row(trip_distance=0.0),
        bronze_row(total_amount=-5.0),
        bronze_row(passenger_count=12),
        bronze_row(pulocationid=None),
    ]
    reasons = [r["reject_reason"] for r in with_reject_reason(conform(make_bronze(spark, rows))).collect()]
    assert reasons == [
        None,
        "pickup_outside_month",
        "duration_out_of_range",
        "distance_out_of_range",
        "negative_amount",
        "bad_passenger_count",
        "missing_location",
    ]


def test_trip_id_is_stable_and_dedupes(spark):
    df = conform(
        make_bronze(spark, [bronze_row(), bronze_row(), bronze_row(fare_amount=19.0, total_amount=27.5)])
    )
    assert df.select("trip_id").distinct().count() == 2
    assert deduplicate(df).count() == 2


def test_quality_checks_flag_problems(spark):
    silver = conform(make_bronze(spark, [bronze_row(), bronze_row()]))
    results = {r.name: r for r in run_checks(silver, ["2024-01"], input_rows=100, rejected_rows=10)}
    assert results["row_count_positive"].passed
    assert not results["trip_id_unique"].passed
    assert not results["every_day_present"].passed
    assert not results["reject_rate"].passed
