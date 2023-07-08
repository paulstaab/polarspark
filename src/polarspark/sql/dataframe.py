from typing import Self

import polars as pl
import pandas as pd

from polarspark.sql.column import Column


class DataFrame:
    __data: pl.DataFrame

    def __init__(self, data: pl.DataFrame) -> None:
        self.__data = data

    def __getattr__(self, name: str) -> Column:
        return Column(name)

    def __getitem__(self, item: int) -> Column:
        return Column(self.columns[item])

    def count(self) -> int:
        return self.__data.shape[0]

    @property
    def columns(self) -> list[str]:
        return self.__data.columns

    @staticmethod
    def _convert_col(col: str | Column) -> pl.Expr:
        if isinstance(col, str):
            return pl.col(col)

        if isinstance(col, Column):
            return col.to_polars()

        raise ValueError(f"Can not use {type(col)} as column: {col}")

    def select(self, *cols: list[str | Column] | str | Column) -> Self:
        cols_polars: list[pl.Expr] = []
        for col in cols:
            if isinstance(col, list):
                cols_polars.extend([self._convert_col(c) for c in col])
            else:
                cols_polars.append(self._convert_col(col))

        return DataFrame(self.__data.select(*cols_polars))

    def toPandas(self) -> pd.DataFrame:
        return self.__data.to_pandas()
