from pyspark.sql import SparkSession

def start_spark_cluster():
    print("Starting spark cluster ..... ")
    return SparkSession.builder.appName("automate_release").getOrCreate()

