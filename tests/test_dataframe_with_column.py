import pandas as pd

from tests import assert_df_transformation_equal


def test_with_column(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, f: df.withColumn("dd", f.lit(5)))
    assert_df_transformation_equal(test_data, lambda df, f: df.withColumn("cc", f.lit(None)))
    assert_df_transformation_equal(
        test_data, lambda df, f: df.withColumn("ee", f.lit("a").alias("aa"))
    )


def test_with_columns(test_data: pd.DataFrame):
    assert_df_transformation_equal(
        test_data, lambda df, f: df.withColumns({"c": f.col("a"), "d": f.lit("d").alias("e")})
    )
