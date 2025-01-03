import pandas as pd

from tests import assert_df_transformation_equal


def test_sum_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.sum("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.sum("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.sum("c")))


def test_min_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.min("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.min("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.min("c")))


def test_avg_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.avg("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.avg("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.avg("c")))


def test_count_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.count("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.count("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.count("c")))


def test_stddev_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.stddev("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.stddev("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.stddev("c")))


def test_variance_function(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.variance("a")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.variance("b")))
    assert_df_transformation_equal(test_data, lambda df, f: df.select(f.variance("c")))
