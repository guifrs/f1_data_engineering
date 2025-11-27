import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
import spark_ops


def parse_args() -> argparse.Namespace:
    """
    Parse command-line arguments for the bronze table builder.

    Returns
    -------
    argparse.Namespace
        Parsed arguments with `input` (glob pattern) and `output` (Delta path).
    """
    parser = argparse.ArgumentParser(
        description="Create/overwrite the bronze 'results' Delta table from raw CSV files."
    )
    parser.add_argument(
        "--input",
        type=str,
        default="../data/raw/*.csv",
        help="Glob pattern for raw CSV files (e.g. '../data/raw/*.csv').",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="../data/bronze/results",
        help="Output path for the bronze Delta table (default: ../data/bronze/results).",
    )
    return parser.parse_args()


def resolve_csv_files(pattern: str) -> list[str]:
    """
    Resolve a glob pattern for CSV input files using pathlib.

    The pattern may be relative to this script's directory.

    Parameters
    ----------
    pattern : str
        Glob pattern for the CSV files (e.g. '../data/raw/*.csv').

    Returns
    -------
    List[str]
        List of absolute file paths matching the pattern.

    Raises
    ------
    FileNotFoundError
        If no CSV files match the given pattern.
    """
    raw_pattern = Path(pattern)

    if raw_pattern.is_absolute():
        base_dir = raw_pattern.parent
        glob_pattern = raw_pattern.name
    else:
        script_dir = Path(__file__).resolve().parent
        full_pattern = (script_dir / raw_pattern).resolve()
        base_dir = full_pattern.parent
        glob_pattern = full_pattern.name

    files = sorted(p for p in base_dir.glob(glob_pattern) if p.is_file())

    if not files:
        raise FileNotFoundError(
            f"No CSV files matched pattern '{pattern}' "
            f"(resolved as '{base_dir / glob_pattern}')."
        )

    return [str(p) for p in files]


def build_bronze_results(
    spark: SparkSession,
    input_pattern: str,
    output_path: str,
) -> None:
    """
    Read raw CSV files and write them as a Delta bronze table.

    Parameters
    ----------
    spark : SparkSession
        Active SparkSession.
    input_pattern : str
        Glob pattern for the input CSV files.
    output_path : str
        Destination path for the Delta table.
    """
    csv_files = resolve_csv_files(input_pattern)

    df: DataFrame = spark.read.csv(csv_files, sep=";", header=True)

    out_path = Path(output_path)
    if not out_path.is_absolute():
        out_path = (Path(__file__).resolve().parent / out_path).resolve()

    (
        df.coalesce(1)
        .write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(str(out_path))
    )


def main() -> None:
    """Entry point for building the bronze 'results' Delta table."""
    args = parse_args()
    spark = spark_ops.new_spark_session()
    build_bronze_results(spark, args.input, args.output)


if __name__ == "__main__":
    main()
