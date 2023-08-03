import pandas as pd

from tests import assert_df_transformation_equal


def test_filter(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.filter(f.col("a") == 2))
    assert_df_transformation_equal(test_data, lambda df, f: df[df.a != 2])
    assert_df_transformation_equal(test_data, lambda df, f: df[df["c"] == 1])
    assert_df_transformation_equal(test_data, lambda df, f: df.filter(f.col("b") >= 2))
    assert_df_transformation_equal(
        test_data, lambda df, f: df.filter((f.col("a") >= 2) | (f.col("b") <= 3))
    )
    assert_df_transformation_equal(test_data, lambda df, f: df.filter("a = 2"))


def test_where(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.where("a = 2 and b <= 3.0"))
