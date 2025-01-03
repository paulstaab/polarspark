import pandas as pd

from polarspark.sql import SparkSession
from tests import assert_df_transformation_equal


def test_create_dataframe_from_pandas(test_data: pd.DataFrame) -> None:
    assert_df_transformation_equal(test_data, lambda df, _: df)


def test_create_from_list():
    # given
    spark = SparkSession.builder.getOrCreate()
    # when
    df = spark.createDataFrame(data=[(2, "Alice"), (5, "Bob")], schema=["age", "name"])
    # then
    assert df.count() == 2
    assert df.columns == ["age", "name"]
