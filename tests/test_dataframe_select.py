import pandas as pd

from tests import assert_df_transformation_equal


def test_select_columns() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, _: df.select("c", "a"))
    assert_df_transformation_equal(data, lambda df, _: df.select("*"))
    assert_df_transformation_equal(
        data, lambda df, f: df.select(f.col("a"), f.col("b").alias("d"), "c")
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.select([f.col("a"), f.col("b"), "c"])
    )
    assert_df_transformation_equal(data, lambda df, _: df[["a", "b"]])


def test_select_expressions():
    # given
    data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [22, 31]})
    # then
    assert_df_transformation_equal(
        data, lambda df, _: df.select(df.name, (df.age * 2 + 10).alias("age"))
    )
