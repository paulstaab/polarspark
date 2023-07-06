import pandas as pd
import polars as pl

from polarspark.sql.dataframe import DataFrame


class SparkSession:
    class Builder:
        def getOrCreate(self):
            return SparkSession()

    builder = Builder()

    def createDataFrame(self, data: pd.DataFrame) -> DataFrame:
        return DataFrame(pl.from_pandas(data=data))
