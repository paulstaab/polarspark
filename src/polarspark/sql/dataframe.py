from typing import Iterable, Self

import polars as pl
import pandas as pd

from polarspark.sql.column import Column


class DataFrame:
    __lazy_data: pl.LazyFrame
    __collected_data: pl.DataFrame | None = None

    def __init__(self, data: pl.DataFrame | pl.LazyFrame) -> None:
        if isinstance(data, pl.DataFrame):
            self.__collected_data = data
            self.__lazy_data = data.lazy()
        else:
            self.__lazy_data = data

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

    @property
    def _lazy_data(self) -> pl.LazyFrame:
        return self.__lazy_data

    @property
    def _collected_data(self) -> pl.DataFrame:
        if self.__collected_data is None:
            self.__collected_data = self.__lazy_data.collect()
            self.__lazy_data = self.__collected_data.lazy()
        return self.__collected_data

    def count(self) -> int:
        return self._collected_data.shape[0]

    @property
    def columns(self) -> list[str]:
        return self._lazy_data.columns

    def select(self, *cols: Iterable[str | Column] | str | Column) -> "DataFrame":
        cols_polars: list[pl.Expr] = []
        for col in cols:
            if isinstance(col, str) or isinstance(col, Column):
                cols_polars.append(col)
            else:
                cols_polars.extend(col)

        return DataFrame(self._lazy_data.select(*cols_polars))

    def filter(self, condition: Column | str) -> "DataFrame":
        if isinstance(condition, Column):
            return DataFrame(self._lazy_data.filter(condition))

        elif isinstance(condition, str):
            ctx = pl.SQLContext(df=self._lazy_data)
            return DataFrame(ctx.execute(f"SELECT * FROM df WHERE {condition}"))

        else:
            raise ValueError(f"Invalid argument for df.filter: {condition}")

    def where(self, condition: Column | str) -> "DataFrame":
        return self.filter(condition)

    def persist(self, *_args, **_kwargs) -> "DataFrame":
        _ = self._collected_data
        return self

    def cache(self) -> "DataFrame":
        return self.persist()

    def localCheckpoint(self) -> "DataFrame":
        return self.persist()

    def toPandas(self) -> pd.DataFrame:
        return self._collected_data.to_pandas()
