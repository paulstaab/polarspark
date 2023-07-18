import pandas as pd

from tests import assert_df_transformation_equal


def test_join() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(
        data, lambda df, f: df.join(df.select("a", f.col("b").alias("d")), "a")
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.join(df.select("c", f.col("b").alias("d")), "c")
    )
    assert_df_transformation_equal(
        data, lambda df, f: df.join(df.select("a", "c", f.col("b").alias("d")), ["a", "c"])
    )
