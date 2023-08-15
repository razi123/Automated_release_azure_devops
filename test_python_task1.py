from pyspark.sql import SparkSession
from pyspark.sql.types import List
import pyspark.sql.functions as f

from src.python_task1 import create_date_time_definition
from utils.utils_assert import assert_pyspark_df_equal

from pyspark import SparkContext

 
def assert_create_date_time_definition(
    spark_session, picks_information_data: List,
    picks_imformation_schema: List,
    expected_data: List,
    expected_schema: List):

    print("2")

    df_picks_information = spark_session.createDataFrame(picks_information_data, picks_imformation_schema)
    actual = create_date_time_definition(spark_session, df_picks_information)

    print("2.2")
    expected = spark_session.createDataFrame(expected_data, expected_schema)
    print("2.3")
    actual.show()
    expected.show()

    assert_pyspark_df_equal(actual.limit(3), expected.limit(3))


def test_create_date_time_definition(spark_session):
    picks_information_schema = ["PICKID", "SVOID", "LFDNR", "AETIMESTAMP", "AENAM_C"]

    print("1") 

    picks_information_data = [
        ("PICKID001", "12345", "123", "2023-07-01 10:25:08", ""),
        ("PICKID002", "12345", "123", "2023-07-01 11:46:07", ""),
        ("PICKID003", "12345", "234", "2023-07-01 12:00:00", ""),
        ("PICKID004", "12345", "234", "2023-07-01 12:45:00", ""),
    ]

    expected_data = [
        ("2023-07-01 08:00:00", "08:00", ""),
        ("2023-07-01 08:30:00", "08:30", ""),
        ("2023-07-01 09:00:00", "09:00", ""),

    ]
    expected_schema = ["time_filter", "time_filter_hour", "status"]

    print("1.1")
    assert_create_date_time_definition(spark_session, picks_information_data, picks_information_schema,
                                       expected_data, expected_schema)
    

if __name__ == "__main__":
    sc = SparkContext(appName="Test_123")
    test_create_date_time_definition(spark_session=SparkSession)