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

    pd.testing.assert_frame_equal(polarspark_df, spark_df)
