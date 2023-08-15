from utils.spark_init import spark
from pyspark.sql.types import List
from src.python_task1 import create_date_time_definition
from utils.utils_assert import assert_pyspark_df_equal


def assert_create_date_time_definition(
    picks_information_data: List,
    picks_imformation_schema: List,
    expected_data: List,
    expected_schema: List):

    df_picks_information = spark.createDataFrame(picks_information_data, picks_imformation_schema)
    actual = create_date_time_definition(spark, df_picks_information)

    expected = spark.createDataFrame(expected_data, expected_schema)
    actual.show(3)
    expected.show(3)

    assert_pyspark_df_equal(actual.limit(3), expected.limit(3))


def test_create_date_time_definition():
    picks_information_schema = ["PICKID", "SVOID", "LFDNR", "AETIMESTAMP"]

    picks_information_data = [
        ("PICKID001", "12345", "123", "2023-07-01 10:25:08",),
        ("PICKID002", "12345", "123", "2023-07-01 11:46:07",),
        ("PICKID003", "12345", "234", "2023-07-01 12:00:00",),
        ("PICKID004", "12345", "234", "2023-07-01 12:45:00",),
    ]

    expected_data = [
        ("2023-07-01 08:00:00", "08:00"),
        ("2023-07-01 08:30:00", "08:30"),
        ("2023-07-01 09:00:00", "09:00"),

    ]
    expected_schema = ["time_filter", "time_filter_hour"]
    assert_create_date_time_definition(picks_information_data, picks_information_schema,
                                       expected_data, expected_schema)

