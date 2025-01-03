import pandas as pd
from polarspark.sql import SparkSession
from tests import assert_df_transformation_equal

def test_show(test_data: pd.DataFrame) -> None:
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(test_data)
    assert_df_transformation_equal(test_data, lambda df, _: df.show())
