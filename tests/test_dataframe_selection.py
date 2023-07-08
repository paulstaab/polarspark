import pandas as pd

from polarspark.sql import SparkSession
from polarspark.sql import functions as f


def test_select_column_as_str() -> None:
    # given
    pandas_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pandas_df)
    # when
    df = df.select("c", "a")
    # then
    assert df.count() == 3
    assert df.columns == ["c", "a"]


def test_select_all_columns() -> None:
    # given
    pandas_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pandas_df)
    # when
    df = df.select("*")
    # then
    assert df.count() == 3
    assert df.columns == ["a", "b", "c"]


def test_select_with_column():
    # given
    pandas_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pandas_df)
    # when
    df = df.select(f.col("a"), f.col("b").alias("d"), "c")
    # then
    assert df.count() == 3
    assert df.columns == ["a", "d", "c"]


def test_select_with_list():
    # given
    pandas_df = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(pandas_df)
    # when
    df = df.select("a", [f.col("b").alias("d"), f.col("c")])
    # then
    assert df.count() == 3
    assert df.columns == ["a", "d", "c"]


def test_select_with_expr():
    # given
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    # when
    df = df.select(df.name, (df.age + 10).alias("age"))
    # then
    pd.testing.assert_frame_equal(
        df.toPandas(), pd.DataFrame({"name": ["Alice", "Bob"], "age": [12, 15]})
    )
