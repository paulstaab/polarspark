import pandas as pd

from tests import assert_df_transformation_equal


def test_with_column() -> None:
    # given
    data = pd.DataFrame(
        {
            "a": [1, 2, 3],
            "b": [2.0, 3.0, 4.0],
            "c": [1, 1, 1],
        }
    )
    # then
    assert_df_transformation_equal(data, lambda df, f: df.withColumn("d", f.lit(5)))
    assert_df_transformation_equal(data, lambda df, f: df.withColumn("c", f.lit(None)))
    assert_df_transformation_equal(data, lambda df, f: df.withColumn("e", f.lit("a").alias("f")))


def test_with_columns():
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
        data, lambda df, f: df.withColumns({"c": f.col("a"), "d": f.lit("d").alias("e")})
    )
