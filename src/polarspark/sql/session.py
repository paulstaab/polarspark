import pandas as pd
import polars as pl

from polarspark.sql.dataframe import DataFrame


class SparkSession:
    class Builder:
        def getOrCreate(self):
            return SparkSession()

    builder = Builder()

    def createDataFrame(
        self, data: pd.DataFrame | list[tuple], schema: list[str] | None = None
    ) -> DataFrame:
        if isinstance(data, pd.DataFrame):
            return DataFrame(pl.from_pandas(data=data))

        if isinstance(data, list):
            return DataFrame(pl.from_records(data, schema))

        raise ValueError(f"Invalid data format: {type(data)}")
