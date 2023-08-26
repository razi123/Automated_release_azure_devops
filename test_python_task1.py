from utils.spark_init import spark
from pyspark.sql.types import List
import pyspark.sql.functions as f

from pyspark.sql.types import StringType, StructField, StructType 

from src.python_task1 import create_date_time_definition
from utils.utils_assert import assert_pyspark_df_equal
from utils.spark_cluster import start_spark_cluster

   
def test_create_date_time_definition(spark: SparkSession):
    picks_information_schema = StructType([StructField("AETIMESTAMP", StringType(), True)])

    print("1") 

    picks_information_data = [
        ("2023-07-01 08:00:08",),
        ("2023-07-01 08:30:07",),
        ("2023-07-01 09:00:00",),
    ]

    expected_data = [
        ("08:00",),
        ("08:30",),
        ("09:00",),
    ]
    
    expected_schema = StructType([
        StructField("time_filter_hour", StringType(), True)
    ])

    df_picks_information = spark.createDataFrame(picks_information_data, picks_information_schema)
    expected = spark.createDataFrame(expected_data, expected_schema)
    actual = create_date_time_definition(df_picks_information)

    actual.show()
    expected.show()

    assert_pyspark_df_equal(actual.limit(3), expected.limit(3))
    

if __name__ == "__main__":
    #sc = SparkContext(appName="Test_123")
    spark = start_spark_cluster() 
    test_create_date_time_definition(spark)
    spark.stop()