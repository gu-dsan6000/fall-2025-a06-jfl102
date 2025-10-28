#!/usr/bin/env python3
import argparse
import os
from pathlib import Path
from datetime import datetime

# Only import Spark/Seaborn when needed to keep --skip-spark fast
def _lazy_import_spark():
    from pyspark.sql import SparkSession, functions as F
    from pyspark.sql.functions import regexp_extract, input_file_name, to_timestamp, col
    return SparkSession, F, regexp_extract, input_file_name, to_timestamp, col

def _lazy_import_plot():
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns
    return pd, plt, sns

REPO_ROOT = Path(__file__).resolve().parent
OUT_DIR = REPO_ROOT / "data" / "output"
OUT_DIR.mkdir(parents=True, exist_ok=True)

TIMELINE_CSV = OUT_DIR / "problem2_timeline.csv"
CLUSTER_SUMMARY_CSV = OUT_DIR / "problem2_cluster_summary.csv"
STATS_TXT = OUT_DIR / "problem2_stats.txt"
BAR_PNG = OUT_DIR / "problem2_bar_chart.png"
DENSITY_PNG = OUT_DIR / "problem2_density_plot.png"

def build_spark(master_url: str, app_name: str = "DSAN6000-Problem2"):
    SparkSession, F, regexp_extract, input_file_name, to_timestamp, col = _lazy_import_spark()
    builder = (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        # Reasonable defaults; the AMI/setup script usually wires s3a already
        .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
        .config("spark.sql.execution.arrow.pyspark.enabled","true")
    )
    spark = builder.getOrCreate()
    return spark

def load_and_parse_logs(spark, s3_bucket_uri: str):
    """
    Reads ALL text logs from your bucket. We don't need to pin to stdout only;
    timestamps appear at the beginning of valid lines.
    """
    SparkSession, F, regexp_extract, input_file_name, to_timestamp, col = _lazy_import_spark()

    # Defensive: accept both s3a://bucket and s3://bucket
    if s3_bucket_uri.startswith("s3://"):
        s3_bucket_uri = s3_bucket_uri.replace("s3://", "s3a://", 1)

    # Pattern: s3a://<bucket>/data/application_*/container_*/*  (stdout, stderr, etc.)
    pattern = f"{s3_bucket_uri}/data/application_*/container_*/*"

    df = spark.read.text(pattern).withColumn("file_path", input_file_name())

    # Extract identifiers from file path
    # application_id (full): application_1485248649253_0001
    # cluster_id:           1485248649253
    # app_number:           0001
    df = (
        df.withColumn("application_id", regexp_extract("file_path", r"(application_\d+_\d+)", 1))
          .withColumn("cluster_id", regexp_extract("file_path", r"application_(\d+)_\d+", 1))
          .withColumn("app_number", regexp_extract("file_path", r"application_\d+_(\d+)", 1))
    )

    # Extract timestamp from the start of line: '17/03/29 10:04:41'
    df = df.withColumn("ts_raw", regexp_extract("value", r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1))
    df = df.withColumn("timestamp", to_timestamp("ts_raw", "yy/MM/dd HH:mm:ss"))

    # Keep only lines that have both application_id and a valid timestamp
    df = df.filter((F.length("application_id") > 0) & (col("timestamp").isNotNull()))

    return df

def build_timeline(df):
    SparkSession, F, *_ = _lazy_import_spark()
    # One row per application_id with min/max timestamp
    timeline = (
        df.groupBy("cluster_id", "application_id", "app_number")
          .agg(F.min("timestamp").alias("start_time"),
               F.max("timestamp").alias("end_time"))
    )
    # Convert to pandas for single-file CSV writing
    pdf = timeline.orderBy("cluster_id", "app_number").toPandas()
    # Ensure consistent ISO formatting
    pdf["start_time"] = pdf["start_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    pdf["end_time"]   = pdf["end_time"].dt.strftime("%Y-%m-%d %H:%M:%S")
    pdf.to_csv(TIMELINE_CSV, index=False)
    return pdf

def build_cluster_summary(timeline_pdf):
    import pandas as pd
    # Aggregations at cluster level
    # Convert times back if needed
    tmp = timeline_pdf.copy()
    tmp["start_time"] = pd.to_datetime(tmp["start_time"])
    tmp["end_time"]   = pd.to_datetime(tmp["end_time"])

    grp = (
        tmp.groupby("cluster_id", as_index=False)
           .agg(num_applications=("application_id", "nunique"),
                cluster_first_app=("start_time", "min"),
                cluster_last_app=("end_time", "max"))
           .sort_values("num_applications", ascending=False)
    )
    # Format times for CSV
    grp["cluster_first_app"] = grp["cluster_first_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
    grp["cluster_last_app"]  = grp["cluster_last_app"].dt.strftime("%Y-%m-%d %H:%M:%S")
    grp.to_csv(CLUSTER_SUMMARY_CSV, index=False)
    return grp

def write_stats(timeline_pdf, cluster_summary_pdf):
    import pandas as pd
    tmp = timeline_pdf.copy()
    tmp["start_time"] = pd.to_datetime(tmp["start_time"])
    tmp["end_time"]   = pd.to_datetime(tmp["end_time"])
    tmp["duration_sec"] = (tmp["end_time"] - tmp["start_time"]).dt.total_seconds().astype(float)

    total_clusters = cluster_summary_pdf["cluster_id"].nunique()
    total_apps = tmp["application_id"].nunique()
    avg_apps_per_cluster = (total_apps / total_clusters) if total_clusters else 0.0

    top_lines = []
    top_lines.append(f"Total unique clusters: {total_clusters}")
    top_lines.append(f"Total applications: {total_apps}")
    top_lines.append(f"Average applications per cluster: {avg_apps_per_cluster:.2f}")
    top_lines.append("")
    top_lines.append("Most heavily used clusters:")

    for _, row in cluster_summary_pdf.head(10).iterrows():
        top_lines.append(f"  Cluster {row['cluster_id']}: {int(row['num_applications'])} applications")

    STATS_TXT.write_text("\n".join(top_lines), encoding="utf-8")
    return top_lines

def make_plots(cluster_summary_pdf, timeline_pdf):
    pd, plt, sns = _lazy_import_plot()

    # --- Bar chart: apps per cluster ---
    fig1 = plt.figure(figsize=(10, 6))
    ax = plt.gca()
    cs = cluster_summary_pdf.sort_values("num_applications", ascending=False)
    ax.bar(cs["cluster_id"].astype(str), cs["num_applications"])
    for i, v in enumerate(cs["num_applications"].tolist()):
        ax.text(i, v, str(int(v)), ha="center", va="bottom", fontsize=9)
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Number of Applications")
    ax.set_title("Applications per Cluster")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    fig1.savefig(BAR_PNG, dpi=150)
    plt.close(fig1)

    # --- Density plot: durations for largest cluster ---
    tdf = timeline_pdf.copy()
    tdf["start_time"] = pd.to_datetime(tdf["start_time"])
    tdf["end_time"]   = pd.to_datetime(tdf["end_time"])
    tdf["duration_sec"] = (tdf["end_time"] - tdf["start_time"]).dt.total_seconds().astype(float)

    if not cluster_summary_pdf.empty:
        largest = cluster_summary_pdf.sort_values("num_applications", ascending=False).iloc[0]["cluster_id"]
        subset = tdf[tdf["cluster_id"] == largest].copy()

        if not subset.empty:
            fig2 = plt.figure(figsize=(10, 6))
            ax2 = plt.gca()
            # Histogram + KDE
            sns.histplot(subset["duration_sec"], kde=True, ax=ax2)
            ax2.set_xscale("log")  # log scale to handle skew
            ax2.set_xlabel("Job Duration (seconds, log scale)")
            ax2.set_ylabel("Count")
            ax2.set_title(f"Job Duration Distribution (Cluster {largest}, n={len(subset)})")
            plt.tight_layout()
            fig2.savefig(DENSITY_PNG, dpi=150)
            plt.close(fig2)

def regenerate_from_existing():
    pd, _, _ = _lazy_import_plot()
    timeline_pdf = pd.read_csv(TIMELINE_CSV)
    cluster_summary_pdf = pd.read_csv(CLUSTER_SUMMARY_CSV)
    write_stats(timeline_pdf, cluster_summary_pdf)
    make_plots(cluster_summary_pdf, timeline_pdf)

def print_outputs_to_stdout():
    # Handy when you just need to paste into files
    if TIMELINE_CSV.exists():
        print(f"\n=== {TIMELINE_CSV.name} ===")
        print(TIMELINE_CSV.read_text(encoding="utf-8"))
    if CLUSTER_SUMMARY_CSV.exists():
        print(f"\n=== {CLUSTER_SUMMARY_CSV.name} ===")
        print(CLUSTER_SUMMARY_CSV.read_text(encoding="utf-8"))
    if STATS_TXT.exists():
        print(f"\n=== {STATS_TXT.name} ===")
        print(STATS_TXT.read_text(encoding="utf-8"))

def main():
    parser = argparse.ArgumentParser(description="Problem 2: Cluster Usage Analysis")
    parser.add_argument("master", nargs="?", help="Spark master URL, e.g. spark://10.0.0.1:7077")
    parser.add_argument("--net-id", required=False, help="Your net ID (for logging)")
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark: regenerate stats/plots from existing CSVs")
    parser.add_argument("--print-outputs", action="store_true",
                        help="Print CSV/txt contents to stdout for manual copy/paste")
    args = parser.parse_args()

    if args.skip_spark:
        regenerate_from_existing()
        if args.print_outputs:
            print_outputs_to_stdout()
        return

    if not args.master:
        raise SystemExit("ERROR: Provide the Spark master URL (e.g., spark://$MASTER_PRIVATE_IP:7077) or use --skip-spark")

    s3_bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not s3_bucket:
        raise SystemExit("ERROR: SPARK_LOGS_BUCKET env var is not set. Export it as instructed in the README.")

    spark = build_spark(args.master, app_name=f"Problem2-{args.net_id or 'student'}")
    try:
        df = load_and_parse_logs(spark, s3_bucket)
        timeline_pdf = build_timeline(df)
        cluster_summary_pdf = build_cluster_summary(timeline_pdf)
        write_stats(timeline_pdf, cluster_summary_pdf)
        make_plots(cluster_summary_pdf, timeline_pdf)
    finally:
        spark.stop()

    if args.print_outputs:
        print_outputs_to_stdout()

if __name__ == "__main__":
    main()

   
