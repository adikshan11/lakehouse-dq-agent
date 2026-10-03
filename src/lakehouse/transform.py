from dataclasses import dataclass

from delta.tables import DeltaTable
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from lakehouse.config import Paths

KEY_COLUMNS = ["vendor_id", "pickup_ts", "dropoff_ts", "pu_location_id", "do_location_id", "total_amount"]


def reject_rules() -> dict[str, Column]:
    return {
        "pickup_outside_month": F.date_format("pickup_ts", "yyyy-MM") != F.col("ingest_month"),
        "duration_out_of_range": ~F.col("trip_minutes").between(1, 240),
        "distance_out_of_range": ~F.col("trip_distance").between(0.01, 200),
        "negative_amount": (F.col("fare_amount") < 0) | (F.col("total_amount") < 0),
        "bad_passenger_count": F.col("passenger_count").isNotNull() & ~F.col("passenger_count").between(0, 8),
        "missing_location": F.col("pu_location_id").isNull() | F.col("do_location_id").isNull(),
    }


@dataclass
class TransformResult:
    input_rows: int
    rejected_rows: int
    duplicate_rows: int
    merged_rows: int


def conform(bronze: DataFrame) -> DataFrame:
    return (
        bronze.select(
            F.col("vendorid").alias("vendor_id"),
            F.col("tpep_pickup_datetime").alias("pickup_ts"),
            F.col("tpep_dropoff_datetime").alias("dropoff_ts"),
            "passenger_count",
            "trip_distance",
            F.col("ratecodeid").alias("rate_code_id"),
            F.col("pulocationid").alias("pu_location_id"),
            F.col("dolocationid").alias("do_location_id"),
            "payment_type",
            "fare_amount",
            "tip_amount",
            "tolls_amount",
            "total_amount",
            "congestion_surcharge",
            "airport_fee",
            "ingest_month",
        )
        .withColumn("trip_minutes", (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0)
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn(
            "trip_id",
            F.sha2(
                F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in KEY_COLUMNS]), 256
            ),
        )
    )


def with_reject_reason(df: DataFrame) -> DataFrame:
    reason = F.lit(None).cast("string")
    for name, rule in reversed(list(reject_rules().items())):
        reason = F.when(F.coalesce(rule, F.lit(True)), F.lit(name)).otherwise(reason)
    return df.withColumn("reject_reason", reason)


def deduplicate(df: DataFrame) -> DataFrame:
    latest_first = Window.partitionBy("trip_id").orderBy(F.col("ingest_month").desc())
    return df.withColumn("_rank", F.row_number().over(latest_first)).where("_rank = 1").drop("_rank")


def merge_into(spark: SparkSession, df: DataFrame, path: str, partition: str) -> None:
    if not DeltaTable.isDeltaTable(spark, path):
        df.write.format("delta").partitionBy(partition).save(path)
        return
    (
        DeltaTable.forPath(spark, path)
        .alias("t")
        .merge(df.alias("s"), "t.trip_id = s.trip_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def build_silver(spark: SparkSession, paths: Paths, months: list[str]) -> TransformResult:
    bronze = spark.read.format("delta").load(str(paths.bronze)).where(F.col("ingest_month").isin(months))
    checked = with_reject_reason(conform(bronze)).cache()

    input_rows = checked.count()
    rejected = checked.where(F.col("reject_reason").isNotNull())
    accepted = checked.where(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected_rows = rejected.count()

    unique = deduplicate(accepted).cache()
    accepted_rows = input_rows - rejected_rows
    merged_rows = unique.count()

    rejected.write.format("delta").mode("overwrite").option(
        "replaceWhere", " OR ".join(f"ingest_month = '{m}'" for m in months)
    ).partitionBy("ingest_month").save(str(paths.quarantine))
    merge_into(spark, unique, str(paths.silver), "pickup_date")

    checked.unpersist()
    unique.unpersist()
    return TransformResult(input_rows, rejected_rows, accepted_rows - merged_rows, merged_rows)
