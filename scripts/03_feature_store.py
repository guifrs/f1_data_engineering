"""CLI tool for running feature-store incremental ingestions using Delta Lake."""

import argparse
from typing import List

from pyspark.sql import functions as F, SparkSession
from rich.console import Console

import spark_ops

console = Console()


def exec_range(query_path: str, start: str, stop: str, spark: SparkSession) -> None:
    """
    Execute an incremental ingestion for all event dates in the given range.

    Parameters
    ----------
    query_path : str
        Path to the SQL template file to run per date.
    start : str
        Start date (YYYY-MM-DD).
    stop : str
        End date (YYYY-MM-DD).
    spark : SparkSession
        Active Spark session.
    """
    dates: List[str] = (
        spark.table("results")
        .filter(f"to_date(date) >= '{start}' AND to_date(date) <= '{stop}'")
        .select(F.to_date("date").alias("dt"))
        .distinct()
        .orderBy("dt")
        .toPandas()["dt"]
        .astype(str)
        .tolist()
    )

    ingestor = spark_ops.IngestorFS(query_path, spark)
    ingestor.exec(dates, compact=False)


def main() -> None:
    """
    Main entrypoint for the feature store ingestion runner.

    Steps:
      1. Parse arguments.
      2. Initialize Spark.
      3. Register bronze/silver base views.
      4. Execute incremental ingestion across the date range.
    """
    parser = argparse.ArgumentParser(
        description="Run feature-store incremental ingestion over a date range."
    )
    parser.add_argument("--query", type=str, default="", help="SQL template path.")
    parser.add_argument("--start", type=str, default="2025-01-01", help="Start date.")
    parser.add_argument("--stop", type=str, default="2026-01-01", help="End date.")
    args = parser.parse_args()

    if not args.query:
        return

    spark = spark_ops.new_spark_session()

    # Required base views
    spark_ops.create_view_from_path("data/bronze/results", spark)

    exec_range(args.query, args.start, args.stop, spark)


if __name__ == "__main__":
    main()
