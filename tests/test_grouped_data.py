import pandas as pd

from tests import assert_df_transformation_equal


def test_grouped_data_min(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("a").min("b"))
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("c").min("a"))


def test_grouped_data_avg(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("a").avg("b"))
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("c").avg("a"))


def test_grouped_data_count(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("a").count())
    assert_df_transformation_equal(test_data, lambda df, f: df.groupby("c").count())
