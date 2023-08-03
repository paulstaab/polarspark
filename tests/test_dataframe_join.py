import pandas as pd

from tests import assert_df_transformation_equal


def test_join(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(
        test_data, lambda df, f: df.join(df.select("a", f.col("b").alias("bb")), "a")
    )
    assert_df_transformation_equal(
        test_data, lambda df, f: df.join(df.select("c", f.col("b").alias("bb")), "c")
    )
    assert_df_transformation_equal(
        test_data, lambda df, f: df.join(df.select("a", "c", f.col("b").alias("bb")), ["a", "c"])
    )
