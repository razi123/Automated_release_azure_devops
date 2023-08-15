import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession
import math


def create_date_time(
    spark: SparkSession, from_timestamp: int, to_timestamp: int, tz: str = "CET"
) -> DataFrame:
    interval_seconds = to_timestamp - from_timestamp
    num_intervals = math.floor(interval_seconds / 1800)
    print("4")
    # round the timestamp to 30mins
    from_timestamp = from_timestamp - (from_timestamp % 1800)
    timestamp = f.to_utc_timestamp(
        f.from_unixtime(f.lit(from_timestamp + f.col("id") * 1800)), tz
    )
    return (
        spark.range(0, num_intervals)
        .withColumn("time_filter", f.date_format(timestamp, "yyyy-MM-dd HH:mm:ss"))
        .withColumn("time_filter_hour", f.date_format(timestamp, "HH:mm"))
        .drop("id")
    )


def create_date_time_definition(
    spark: SparkSession, picks_information: DataFrame
) -> DataFrame:
    current_timestamp = f.unix_timestamp()
    current_date = f.to_date(f.from_unixtime(current_timestamp))
    current_time = f.date_format(f.from_unixtime(current_timestamp), "HH:mm:ss")

    timestamps = (
        picks_information.select(
            f.unix_timestamp(f.min(f.col("AETIMESTAMP"))).alias("from_timestamp"))
        .withColumn("to_timestamp", current_timestamp)
        .collect()
    )
    from_timestamp, to_timestamp = timestamps[0][0], timestamps[0][1]

    date_time = create_date_time(spark, from_timestamp, to_timestamp)
    date_time = date_time.withColumn("date", f.to_date("time_filter"))
    date_time = date_time.withColumn("time", f.date_format("time_filter", "HH:mm:ss"))

    return date_time.drop("date", "time")
