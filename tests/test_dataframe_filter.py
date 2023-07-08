import pandas as pd

from tests import assert_df_transformation_equal


def test_filter() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.filter(f.col("a") == 2))
    assert_df_transformation_equal(data, lambda df, f: df[df.a != 2])
    assert_df_transformation_equal(data, lambda df, f: df[df["c"] == 1])
    assert_df_transformation_equal(data, lambda df, f: df.filter(f.col("b") >= 2))
    assert_df_transformation_equal(
        data, lambda df, f: df.filter((f.col("a") >= 2) | (f.col("b") <= 3))
    )
    assert_df_transformation_equal(data, lambda df, f: df.filter("a = 2"))


def test_where() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.where("a = 2 and b <= 3.0"))
