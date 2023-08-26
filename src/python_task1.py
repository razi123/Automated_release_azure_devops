import pyspark.sql.functions as f
from pyspark.sql import DataFrame, SparkSession


def create_date_time_definition(picks_information: DataFrame
) -> DataFrame:

    picks_information = picks_information.withColumn("time_filter_hour", f.substring(f.col("AETIMESTAMP"), 12, 5))

    return picks_information.drop("AETIMESTAMP",)
