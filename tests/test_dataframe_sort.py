import pandas as pd

from tests import assert_df_transformation_equal


def test_sort_single_column(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.sort("a"))
    assert_df_transformation_equal(test_data, lambda df, f: df.sort("a", ascending=False))
    assert_df_transformation_equal(test_data, lambda df, f: df.sort(f.col("a"), ascending=False))
    assert_df_transformation_equal(test_data, lambda df, f: df.sort(f.asc("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.sort(f.desc("a")))


def test_sort_multiple_columns(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.sort("a", "b"))
    assert_df_transformation_equal(test_data, lambda df, f: df.sort(["a", "b"]))
    assert_df_transformation_equal(
        test_data, lambda df, f: df.sort(["a", "b"], ascending=[True, False])
    )
    assert_df_transformation_equal(test_data, lambda df, f: df.sort(f.asc("a"), f.desc("b")))


def test_orderBy_single_column(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy("a"))
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy("a", ascending=False))
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy(f.asc("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy(f.desc("a")))


def test_orderBy_multiple_columns(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy("a", "b"))
    assert_df_transformation_equal(
        test_data, lambda df, f: df.orderBy(["a", "b"], ascending=[True, False])
    )
    assert_df_transformation_equal(test_data, lambda df, f: df.orderBy(f.asc("a"), f.desc("b")))
