import pandas as pd
from datetime import date, datetime

from polarspark.sql import SparkSession


def test_create_dataframe_from_pandas():
    # given
    pandas_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": ["string1", "string2", "string3"],
            "d": [date(2000, 1, 1), date(2000, 2, 1), date(2000, 3, 1)],
            "e": [
                datetime(2000, 1, 1, 12, 0),
                datetime(2000, 1, 2, 12, 0),
                datetime(2000, 1, 3, 12, 0),
            ],
        }
    )
    spark = SparkSession.builder.getOrCreate()
    # when
    df = spark.createDataFrame(pandas_df)
    # then
    assert df.count() == 3
    assert df.columns == ["a", "b", "c", "d", "e"]


def test_create_from_list():
    # given
    spark = SparkSession.builder.getOrCreate()
    # when
    df = spark.createDataFrame(data=[(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    # then
    assert df.count() == 2
    assert df.columns == ["age", "name"]
