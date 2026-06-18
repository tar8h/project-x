import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, col, count, expr, max as spark_max, min as spark_min, stddev, to_timestamp, window


FEATURE_COLUMNS = ["temperature", "vibration", "voltage"]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build batch features from raw sensor lake data.")
    parser.add_argument("--input", default="s3a://sensor-raw/raw/")
    parser.add_argument("--output", default="s3a://sensor-raw/features/hourly/")
    parser.add_argument("--window", default="15 minutes")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = (
        SparkSession.builder.appName("project-x-batch-feature-engineering")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )

    raw = spark.read.json(args.input)
    normalized = (
        raw.withColumn("event_time", to_timestamp(col("ts")))
        .where(col("machine_id").isNotNull())
        .where(col("event_time").isNotNull())
    )

    aggregations = []
    for feature in FEATURE_COLUMNS:
        aggregations.extend(
            [
                avg(feature).alias(f"{feature}_avg"),
                stddev(feature).alias(f"{feature}_std"),
                spark_min(feature).alias(f"{feature}_min"),
                spark_max(feature).alias(f"{feature}_max"),
            ]
        )

    features = (
        normalized.groupBy("machine_id", window("event_time", args.window))
        .agg(count("*").alias("reading_count"), *aggregations)
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
        .withColumn("feature_created_at", expr("current_timestamp()"))
    )

    features.write.mode("overwrite").parquet(args.output)
    spark.stop()


if __name__ == "__main__":
    main()
