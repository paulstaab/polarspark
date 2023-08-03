import pandas as pd

from tests import assert_df_transformation_equal


def test_select_columns(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, _: df.select("c", "a"))
    assert_df_transformation_equal(test_data, lambda df, _: df.select("*"))
    assert_df_transformation_equal(
        test_data, lambda df, f: df.select(f.col("a"), f.col("b").alias("d"), "c")
    )
    assert_df_transformation_equal(
        test_data, lambda df, f: df.select([f.col("a"), f.col("b"), "c"])
    )
    assert_df_transformation_equal(test_data, lambda df, _: df[["a", "b"]])


def test_select_expressions(test_data):
    assert_df_transformation_equal(
        test_data, lambda df, _: df.select(df.a, (df.b * 2 + 10).alias("bb"))
    )
