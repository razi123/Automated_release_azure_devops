from pyspark.sql import SparkSession


def get_spark_session(app_name="Automated_release"):
    """
    Get an existing Spark session if available, otherwise create a new one.
    """
    try:
        # Attempt to retrieve an existing Spark session
        spark = SparkSession.builder.getOrCreate()
        return spark
    except Exception as e:
        print(f"Error getting existing Spark session: {e}")
        # If an error occurs, start a new Spark session
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        return spark


# Use the get_spark_session function to get the Spark session
spark = get_spark_session()

# Your code using the Spark session
