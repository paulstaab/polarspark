from typing import Iterable, Self

import polars as pl
import pandas as pd

from polarspark.sql.column import Column


class DataFrame:
    __data: pl.LazyFrame | pl.DataFrame

    def __init__(self, data: pl.DataFrame | pl.LazyFrame) -> None:
        if isinstance(data, pl.DataFrame):
            data = data.lazy()
        self.__data = data

    def __getattr__(self, name: str) -> Column:
        return pl.col(name)

    def __getitem__(self, item: int | str | Column | list | tuple) -> Column | Self:
        if isinstance(item, int):
            return pl.col(self.columns[item])
        if isinstance(item, str):
            return pl.col(item)
        if isinstance(item, Column):
            return self.filter(item)
        if isinstance(item, list) or isinstance(item, tuple):
            return self.select(item)
        raise ValueError(f"Invalid value of type {type(item)} in DataFrame[ ]: {item}")

    def _collect_data(self) -> pd.DataFrame:
        if isinstance(self.__data, pl.LazyFrame):
            self.__data = self.__data.collect()
        assert isinstance(self.__data, pl.DataFrame)
        return self.__data

    def count(self) -> int:
        return self._collect_data().shape[0]

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

    def filter(self, condition: Column | str) -> Self:
        if isinstance(condition, Column):
            return DataFrame(self.__data.filter(condition))

        elif isinstance(condition, str):
            ctx = pl.SQLContext(df=self.__data)
            return DataFrame(ctx.execute(f"SELECT * FROM df WHERE {condition}"))

        else:
            raise ValueError(f"Invalid argument for df.filter: {condition}")

    def where(self, condition: Column | str) -> Self:
        return self.filter(condition)

    def persist(self, *_args, **_kwargs) -> Self:
        self.__data = self._collect_data().lazy()
        return self

    def cache(self) -> Self:
        return self.persist()

    def localCheckpoint(self) -> Self:
        return DataFrame(self._collect_data())

    def toPandas(self) -> pd.DataFrame:
        return self._collect_data().to_pandas()
