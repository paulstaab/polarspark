from typing import Iterable, Union, TYPE_CHECKING

import polars as pl
import pandas as pd

from polarspark.sql.column import Column

if TYPE_CHECKING:
    from polarspark.sql.group import GroupedData


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

    def __getitem__(self, item: int | str | Column | list | tuple) -> Union[Column, "DataFrame"]:
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
        return self._collected_data.height

    @property
    def columns(self) -> list[str]:
        return self._lazy_data.columns

    def select(self, *cols: Iterable[str | Column] | str | Column) -> "DataFrame":
        return DataFrame(self._lazy_data.select(*cols))

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

    def groupby(self, *cols: list[str | Column] | str | Column) -> "GroupedData":
        from polarspark.sql.group import GroupedData

        return GroupedData(self.__lazy_data.groupby(*cols))

    def groupBy(self, *cols: list[str | Column] | str | Column) -> "GroupedData":
        return self.groupby(*cols)

    def persist(self, *_args, **_kwargs) -> "DataFrame":
        _ = self._collected_data
        return self

    def cache(self) -> "DataFrame":
        return self.persist()

    def localCheckpoint(self) -> "DataFrame":
        return self.persist()

    def toPandas(self) -> pd.DataFrame:
        return self._collected_data.to_pandas()

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        return DataFrame(self._lazy_data.with_columns(**{colName: col}))

    def withColumns(self, colsMap: dict[str, Column]) -> "DataFrame":
        return DataFrame(self._lazy_data.with_columns(**colsMap))
