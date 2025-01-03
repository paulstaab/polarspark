import pandas as pd

from polarspark.sql import SparkSession


def test_show(test_data: pd.DataFrame) -> None:
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame(test_data)
    df.show()
    df.show(3, truncate=False)
    df.show(n=10, truncate=True)
