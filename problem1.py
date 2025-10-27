# problem1.py
# Analyze log level distribution (INFO/WARN/ERROR/DEBUG)
# Outputs:
#   data/output/problem1_counts.csv
#   data/output/problem1_sample.csv
#   data/output/problem1_summary.txt
#
# Usage (local dev default):
#   python problem1.py
#   python problem1.py --input data/sample --output data/output
# Usage (cluster/full later):
#   spark-submit problem1.py --input data/raw --output data/output

import argparse
import os
import re
import shutil
from glob import glob

from pyspark.sql import SparkSession, functions as F

LEVEL_RE = r'\b(INFO|WARN|ERROR|DEBUG)\b'

def write_single_csv(df, final_path):
    """
    Write a single-file CSV with header to `final_path` (not a folder).
    We write to a temp dir, then move the single part file into place.
    """
    tmp_dir = final_path + "_tmp"
    if os.path.exists(final_path):
        os.remove(final_path)
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(tmp_dir)

    # Move part file to final_path
    part_files = glob(os.path.join(tmp_dir, "part-*.csv"))
    if not part_files:
        # Spark may write .csv with .csv or no extension depending on version
        part_files = glob(os.path.join(tmp_dir, "part-*"))
    if not part_files:
        raise RuntimeError(f"No part file written in {tmp_dir}")
    os.makedirs(os.path.dirname(final_path), exist_ok=True)
    shutil.move(part_files[0], final_path)
    shutil.rmtree(tmp_dir)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="data/sample", help="Input dir (default: data/sample)")
    parser.add_argument("--output", default="data/output", help="Output dir (default: data/output)")
    args = parser.parse_args()

    input_path = args.input.rstrip("/")
    out_dir = args.output.rstrip("/")

    os.makedirs(out_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("Problem1-LogLevelDistribution")
        .getOrCreate()
    )

    # Read ALL files recursively under input (works for .log, .gz, etc.)
    # Using DataFrame API so we can use regexp_extract easily.
    df = (
    spark.read
         .option("recursiveFileLookup", "true")
         .text(f"{input_path}/**/*.log*")
)

    # Extract level; keep only rows that matched one of the four levels
    df_levels = df.withColumn("log_level", F.regexp_extract(F.col("value"), LEVEL_RE, 1)) \
                  .filter(F.col("log_level") != "")

    # Counts per level
    counts_df = df_levels.groupBy("log_level").count().orderBy(F.desc("count"))

    # Sample 10 random log lines with their levels
    sample_df = df_levels.select(
        F.col("value").alias("log_entry"),
        F.col("log_level")
    ).orderBy(F.rand()).limit(10)

    # Write outputs as single CSV files (exact filenames per README)
    counts_csv = f"{out_dir}/problem1_counts.csv"
    sample_csv = f"{out_dir}/problem1_sample.csv"
    write_single_csv(counts_df.select(F.col("log_level").alias("log_level"),
                                      F.col("count").alias("count")),
                     counts_csv)
    write_single_csv(sample_df, sample_csv)

    # Summary text
    total_lines = df.count()
    total_matched = df_levels.count()
    by_level = {r["log_level"]: r["count"] for r in counts_df.collect()}
    levels_order = ["INFO", "WARN", "ERROR", "DEBUG"]

    summary_lines = [
        "Problem 1 Summary",
        "==================",
        f"Total lines scanned: {total_lines}",
        f"Total lines with a recognized level: {total_matched}",
        "",
        "Counts by level:"
    ]
    for lvl in levels_order:
        summary_lines.append(f"  {lvl}: {by_level.get(lvl, 0)}")

    with open(f"{out_dir}/problem1_summary.txt", "w") as f:
        f.write("\n".join(summary_lines) + "\n")

    spark.stop()

if __name__ == "__main__":
    main()
