from collections.abc import Callable
from typing import Any, Union

import pandas as pd
from pandas import DataFrame as PandasDF
from pyspark.sql import DataFrame as SparkDF
from pyspark.sql import SparkSession
from pyspark.sql import functions as sparkfunc

from polarspark.sql import (
    DataFrame as PolarsparkDF,
)
from polarspark.sql import (
    SparkSession as PolarsparkSession,
)
from polarspark.sql import (
    functions as polarsparkfunc,
)

DataFrame = Union[SparkDF, PolarsparkDF]  # noqa: UP007


def assert_df_transformation_equal(
    data: PandasDF, transformation: Callable[[DataFrame, Any], DataFrame]
) -> None:
    spark = SparkSession.builder.getOrCreate()
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
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

    # Convert dates and timestamps to the same format
    for col in polarspark_df.columns:
        if polarspark_df[col].dtype.name.startswith("date") or polarspark_df[
            col
        ].dtype.name.startswith("timestamp"):
            polarspark_df[col] = pd.to_datetime(polarspark_df[col]).astype("datetime64[us]")
            spark_df[col] = pd.to_datetime(spark_df[col]).astype("datetime64[us]")

    pd.testing.assert_frame_equal(
        polarspark_df, spark_df, check_dtype=False, check_datetimelike_compat=True
    )
