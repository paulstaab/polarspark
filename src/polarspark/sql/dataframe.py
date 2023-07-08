from typing import Iterable, Self

import polars as pl
import pandas as pd

from polarspark.sql.column import Column


class DataFrame:
    __data: pl.DataFrame

    def __init__(self, data: pl.DataFrame) -> None:
        self.__data = data

    def __getattr__(self, name: str) -> Column:
        return pl.col(name)

    def __getitem__(self, item: int) -> Column:
        return pl.col(self.columns[item])

    def count(self) -> int:
        return self.__data.shape[0]

    @property
    def columns(self) -> list[str]:
        return self.__data.columns

    def select(self, *cols: Iterable[str | Column] | str | Column) -> Self:
        cols_polars: list[pl.Expr] = []
        for col in cols:
            if isinstance(col, str) or isinstance(col, Column):
                cols_polars.append(col)
            else:
                cols_polars.extend(col)

        return DataFrame(self.__data.select(*cols_polars))

    def toPandas(self) -> pd.DataFrame:
        return self.__data.to_pandas()
