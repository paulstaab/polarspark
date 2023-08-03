import pandas as pd

from polarspark.sql import SparkSession


def test_create_dataframe_from_pandas(test_data: pd.DataFrame):
    # given
    spark = SparkSession.builder.getOrCreate()
    # when
    df = spark.createDataFrame(test_data)
    # then
    assert df.count() == len(test_data)
    assert df.columns == list(test_data.columns)


def test_create_from_list():
    # given
    spark = SparkSession.builder.getOrCreate()
    # when
    df = spark.createDataFrame(data=[(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    # then
    assert df.count() == 2
    assert df.columns == ["age", "name"]
