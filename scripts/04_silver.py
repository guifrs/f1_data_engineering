import argparse
from pyspark.sql import SparkSession

import spark_ops


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the silver table builder.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with the `query` attribute representing the
        path to the SQL file used to create the target silver table.
    """
    parser = argparse.ArgumentParser(
        description="Create or refresh a silver Delta table from a SQL query file."
    )
    parser.add_argument(
        "--query",
        type=str,
        default="",
        help="Path to the SQL file used to create the target silver table.",
    )
    return parser.parse_args()


def main() -> None:
    """
    Main execution function for the silver table pipeline.

    It:
      1. Parses the `--query` argument.
      2. Initializes a SparkSession configured with Delta Lake.
      3. Registers base views from bronze/silver paths.
      4. Ensures the `champions` silver table exists.
      5. Creates or refreshes the silver table defined by `--query`.
    """
    args = parse_args()
    query_path = args.query

    if not query_path:
        return

    spark: SparkSession = spark_ops.new_spark_session()

    spark_ops.create_view_from_path("data/bronze/results", spark)
    spark_ops.create_view_from_path("data/silver/feature_store_drivers", spark)

    # Ensure `champions` table exists, creating it if necessary.
    try:
        spark_ops.create_view_from_path("data/silver/champions", spark)
    except Exception:
        spark_ops.create_table("etl/champions.sql", spark)

    # Create or refresh the target silver table from the provided query.
    spark_ops.create_table(query_path, spark)


if __name__ == "__main__":
    main()
