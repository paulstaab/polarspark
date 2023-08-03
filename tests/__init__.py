import pandas as pd
from typing import Union

from pandas import DataFrame as PandasDF
from pyspark.sql import DataFrame as SparkDF, SparkSession, functions as sparkfunc

from typing import Callable, Any

from polarspark.sql import (
    DataFrame as PolarsparkDF,
    SparkSession as PolarsparkSession,
    functions as polarsparkfunc,
)

DataFrame = Union[SparkDF, PolarsparkDF]


def assert_df_transformation_equal(
    data: PandasDF, transformation: Callable[[DataFrame, Any], DataFrame]
) -> None:
    spark = SparkSession.builder.getOrCreate()
    spark_df = transformation(spark.createDataFrame(data), sparkfunc).toPandas()

    polarspark = PolarsparkSession.builder.getOrCreate()
    polarspark_df = transformation(polarspark.createDataFrame(data), polarsparkfunc).toPandas()

    # Ignore row order but enforce identical columns
    spark_cols = list(spark_df.columns)
    polarspark_cols = list(polarspark_df.columns)
    if spark_cols != polarspark_cols:
        raise AssertionError(
            f"Columns do not match. Spark: {spark_cols}. Polarspark: {polarspark_cols}."
        )
    spark_df = spark_df.sort_values(list(spark_df.columns)).reset_index(drop=True)
    polarspark_df = polarspark_df.sort_values(list(polarspark_df.columns)).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        polarspark_df, spark_df, check_dtype=False, check_datetimelike_compat=True
    )
