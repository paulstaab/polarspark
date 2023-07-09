import pandas as pd
import pytest

from tests import assert_df_transformation_equal


def test_group_count() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 1, 2, 2],
            "b": ["a", "b", "a", "b"],
            "c": [1, 1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.groupby("a").count())
    assert_df_transformation_equal(data, lambda df, f: df.groupby("a", "c").count())
    assert_df_transformation_equal(data, lambda df, f: df.groupby(["a", "c"]).count())


def test_group_mean() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 1, 2, 2],
            "b": ["a", "b", "a", "b"],
            "c": [1, 1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.groupby(["a"]).mean("c"))
    assert_df_transformation_equal(data, lambda df, f: df.groupBy(["a"]).avg("c"))
    assert_df_transformation_equal(data, lambda df, f: df.groupBy(["b"]).mean("a", "c"))
    assert_df_transformation_equal(data, lambda df, f: df.groupby(["b"]).avg("a", "c"))


def test_group_agg() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 1, 2, 2],
            "b": ["a", "b", "a", "b"],
            "c": [1, 1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.groupby(["a", "b"]).agg(f.sum("c")))
    assert_df_transformation_equal(
        data, lambda df, f: df.groupby("a").agg(f.sum("c").alias("sum_c"))
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.groupby(["a", "b"]).agg(f.sum(f.col("c")))
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.groupby("a").agg(f.sum(f.col("c")).alias("sum_c"))
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.groupby(["b"]).agg(f.sum("a"), f.max("c"))
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.groupBy(df.b).agg({"a": "sum", "c": "max"})
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.groupBy(df.a).agg(f.sum(f.col("c") + 1).alias("sum_c_plus_1"))
    )
    with pytest.raises(NotImplementedError):
        assert_df_transformation_equal(
            data, lambda df, f: df.groupBy(df.a).agg(f.sum(f.col("c") + 1))
        )
