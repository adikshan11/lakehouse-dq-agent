import time
from pathlib import Path

import requests
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from lakehouse.config import TLC_BASE_URL, ZONES_URL, Paths

BRONZE_COLUMNS = {
    "vendorid": "int",
    "tpep_pickup_datetime": "timestamp",
    "tpep_dropoff_datetime": "timestamp",
    "passenger_count": "int",
    "trip_distance": "double",
    "ratecodeid": "int",
    "store_and_fwd_flag": "string",
    "pulocationid": "int",
    "dolocationid": "int",
    "payment_type": "int",
    "fare_amount": "double",
    "extra": "double",
    "mta_tax": "double",
    "tip_amount": "double",
    "tolls_amount": "double",
    "improvement_surcharge": "double",
    "total_amount": "double",
    "congestion_surcharge": "double",
    "airport_fee": "double",
}


def download(url: str, target: Path, retries: int = 3) -> Path:
    if target.exists() and target.stat().st_size > 0:
        return target
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(target.suffix + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, stream=True, timeout=120) as response:
                response.raise_for_status()
                with open(partial, "wb") as handle:
                    for chunk in response.iter_content(chunk_size=1 << 20):
                        handle.write(chunk)
            partial.rename(target)
            return target
        except requests.RequestException:
            if attempt == retries:
                raise
            time.sleep(2**attempt)
    return target


def download_month(paths: Paths, month: str) -> Path:
    name = f"yellow_tripdata_{month}.parquet"
    return download(f"{TLC_BASE_URL}/{name}", paths.raw / name)


def download_zones(target: Path) -> Path:
    return download(ZONES_URL, target)


def normalize(df: DataFrame) -> DataFrame:
    lowered = df.toDF(*[c.lower() for c in df.columns])
    return lowered.select(
        [
            (F.col(name) if name in lowered.columns else F.lit(None)).cast(dtype).alias(name)
            for name, dtype in BRONZE_COLUMNS.items()
        ]
    )


def ingest_month(spark: SparkSession, paths: Paths, month: str) -> int:
    source = download_month(paths, month)
    bronze = (
        normalize(spark.read.parquet(str(source)))
        .withColumn("ingest_month", F.lit(month))
        .withColumn("source_file", F.lit(source.name))
        .withColumn("ingested_at", F.current_timestamp())
    )
    (
        bronze.write.format("delta")
        .mode("overwrite")
        .option("replaceWhere", f"ingest_month = '{month}'")
        .partitionBy("ingest_month")
        .save(str(paths.bronze))
    )
    return spark.read.format("delta").load(str(paths.bronze)).where(F.col("ingest_month") == month).count()
