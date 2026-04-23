"""
spark-demo — USGS earthquake analytics with PySpark.

Reads the last 30 days of worldwide earthquake data from a CSV
published by the US Geological Survey, aggregates it with PySpark
DataFrame APIs, and writes a handful of result tables to a SQLite
database. Grafana reads those tables to power the dashboard.

The dataset (~10-12k rows) is small enough to run on a laptop in a few
seconds, but the code path is identical to how you'd process billions
of rows on a cluster: lazy transformations, a DataFrame API, explicit
schema, Spark SQL.

Run:  python analyze.py
"""

from __future__ import annotations

import os
import sqlite3
import sys
import time as pytime
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType,
    StructField,
    StructType,
    TimestampType,
    DoubleType,
    IntegerType,
)

HERE = Path(__file__).parent
CSV_PATH = HERE / "data" / "quakes.csv"
DB_PATH = HERE / "output" / "quakes.db"


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("usgs-quake-analytics")
        .master("local[*]")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def load_quakes(spark: SparkSession, csv_path: Path) -> DataFrame:
    """Load the USGS all_month.csv feed with an explicit schema.

    Only the fields we use downstream are kept; the rest of USGS's
    ~22 columns are discarded at read time.
    """
    schema = StructType([
        StructField("time", TimestampType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("depth", DoubleType(), True),
        StructField("mag", DoubleType(), True),
        StructField("magType", StringType(), True),
        StructField("nst", IntegerType(), True),
        StructField("gap", DoubleType(), True),
        StructField("dmin", DoubleType(), True),
        StructField("rms", DoubleType(), True),
        StructField("net", StringType(), True),
        StructField("id", StringType(), True),
        StructField("updated", TimestampType(), True),
        StructField("place", StringType(), True),
        StructField("type", StringType(), True),
    ])
    df = (
        spark.read
        .option("header", "true")
        .schema(schema)
        .csv(str(csv_path))
    )
    # only keep actual earthquakes (drop explosions, ice quakes, etc.)
    # and rows without a magnitude (happens for automated pickings).
    return df.filter((F.col("type") == "earthquake") & F.col("mag").isNotNull())


def aggregate(df: DataFrame) -> dict[str, DataFrame]:
    """Compute the tables Grafana will render."""

    # --- events: a denormalised, dashboard-ready per-row view
    events = df.select(
        F.col("time").alias("time_utc"),
        F.col("mag"),
        F.col("depth"),
        F.col("latitude"),
        F.col("longitude"),
        F.col("place"),
        F.col("magType").alias("mag_type"),
        F.col("net"),
    )

    # --- daily rollups
    daily_counts = (
        df.groupBy(F.to_date("time").alias("day"))
          .agg(
              F.count("*").alias("count"),
              F.max("mag").alias("max_mag"),
              F.avg("mag").alias("avg_mag"),
          )
          .orderBy("day")
    )

    # --- magnitude distribution (integer bucket, e.g. mag 2.3 -> bucket 2)
    mag_buckets = (
        df.withColumn("bucket", F.floor("mag").cast("int"))
          .groupBy("bucket")
          .agg(F.count("*").alias("count"))
          .orderBy("bucket")
    )

    # --- top 10 strongest
    top_strongest = (
        df.orderBy(F.col("mag").desc_nulls_last())
          .limit(10)
          .select(
              F.col("time").alias("time_utc"),
              "mag",
              "depth",
              "place",
          )
    )

    # --- top 10 regions (extracted from "place": USGS formats as
    #     "<distance> <direction> of <region>", sometimes just "<region>".)
    top_places = (
        df.withColumn(
            "region",
            F.when(
                F.col("place").contains(" of "),
                F.regexp_extract(F.col("place"), r" of (.+)$", 1),
            ).otherwise(F.col("place")),
        )
        .groupBy("region")
        .agg(
            F.count("*").alias("count"),
            F.max("mag").alias("max_mag"),
        )
        .orderBy(F.col("count").desc())
        .limit(10)
    )

    # --- headline stats: single-row summary
    summary = df.agg(
        F.count("*").alias("total_events"),
        F.round(F.max("mag"), 1).alias("max_mag"),
        F.round(F.avg("mag"), 2).alias("avg_mag"),
        F.round(F.avg("depth"), 1).alias("avg_depth_km"),
        F.countDistinct(
            F.when(
                F.col("place").contains(" of "),
                F.regexp_extract(F.col("place"), r" of (.+)$", 1),
            ).otherwise(F.col("place"))
        ).alias("distinct_regions"),
    )

    return {
        "events": events,
        "daily_counts": daily_counts,
        "mag_buckets": mag_buckets,
        "top_strongest": top_strongest,
        "top_places": top_places,
        "summary": summary,
    }


def write_sqlite(tables: dict[str, DataFrame], db_path: Path) -> None:
    """Write every Spark DataFrame as a SQLite table via pandas.

    The dataset is small (~10k rows), so the toPandas round-trip is
    cheap. For a bigger dataset we'd use Spark JDBC with a real DB.
    """
    db_path.parent.mkdir(parents=True, exist_ok=True)
    if db_path.exists():
        db_path.unlink()
    con = sqlite3.connect(db_path)
    try:
        for name, df in tables.items():
            pdf = df.toPandas()
            # Grafana's SQLite plugin reads ISO-8601 strings for time.
            for col, dtype in pdf.dtypes.items():
                if "datetime" in str(dtype):
                    pdf[col] = pdf[col].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
            pdf.to_sql(name, con, if_exists="replace", index=False)
            print(f"  wrote {name:<15s} rows={len(pdf):>5d}")
        con.commit()
    finally:
        con.close()


def main() -> int:
    if not CSV_PATH.exists():
        print(f"error: {CSV_PATH} not found. Run `curl -fsSL -o data/quakes.csv "
              "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_month.csv`",
              file=sys.stderr)
        return 1

    t0 = pytime.time()
    spark = build_spark()
    try:
        df = load_quakes(spark, CSV_PATH).cache()
        total = df.count()
        print(f"loaded {total:,} earthquakes from {CSV_PATH.name}")
        tables = aggregate(df)
        write_sqlite(tables, DB_PATH)
        tables["summary"].show(truncate=False)
    finally:
        spark.stop()
    print(f"done in {pytime.time() - t0:.1f}s -> {DB_PATH}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
