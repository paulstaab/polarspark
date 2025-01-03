from collections.abc import Iterable
from typing import TYPE_CHECKING, Union

import pandas as pd
import polars as pl

from polarspark.sql import functions as f
from polarspark.sql.column import Column

if TYPE_CHECKING:
    from pyspark.sql import DataFrame as SparkDataFrame

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
        return Column(pl.col(name))

    def __getitem__(self, item: int | str | Column | list | tuple) -> Union[Column, "DataFrame"]:
        if isinstance(item, int):
            return Column(pl.col(self.columns[item]))
        if isinstance(item, str):
            return Column(pl.col(item))
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
        if len(cols) == 1 and not (isinstance(cols[0], str) or isinstance(cols[0], Column)):
            cols = list(cols[0])
        return DataFrame(self._lazy_data.select(*[f.col(c).expr for c in cols]))  # type: ignore

    def filter(self, condition: Column | str) -> "DataFrame":
        if isinstance(condition, Column):
            return DataFrame(self._lazy_data.filter(condition.expr))

        elif isinstance(condition, str):
            ctx = pl.SQLContext(df=self._lazy_data)
            return DataFrame(ctx.execute(f"SELECT * FROM df WHERE {condition}"))

        else:
            raise ValueError(f"Invalid argument for df.filter: {condition}")

    def where(self, condition: Column | str) -> "DataFrame":
        return self.filter(condition)

    def groupby(self, *cols: list[str | Column] | str | Column) -> "GroupedData":
        from polarspark.sql.group import GroupedData

        return GroupedData(self.__lazy_data.group_by(*cols))

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
        return self._collected_data.to_pandas(use_pyarrow_extension_array=True)

    def toSpark(self) -> "SparkDataFrame":
        from pyspark.sql import SparkSession

        spark = SparkSession.builder.getOrCreate()
        spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
        return spark.createDataFrame(self._collected_data.to_pandas())

    def withColumn(self, colName: str, col: Column) -> "DataFrame":
        return DataFrame(self._lazy_data.with_columns(**{colName: col}))

    def withColumns(self, colsMap: dict[str, Column]) -> "DataFrame":
        return DataFrame(self._lazy_data.with_columns(**colsMap))

    def join(self, other: "DataFrame", on: str | list[str], how: str = "inner") -> "DataFrame":
        if isinstance(on, str):
            on = [on]
        cols_left = [c for c in self.columns if c not in on]
        cols_right = [c for c in other.columns if c not in on]
        if duplicated_cols := set(cols_left).intersection(cols_right):
            raise ValueError(
                f"Columns {duplicated_cols} would be duplicated by this join in pyspark, as they "
                f"are included in both dataframes, but are not part of the join columns. This is "
                "not possible with polarspark. Remove or rename them in one of the dataframes."
            )

        # execute the actual join
        joined_df = self._lazy_data.join(other._lazy_data, on, how)

        # adjust the column order to what pyspark would create
        # potential bug in polars: Why do we need the collect here?
        joined_df = joined_df.collect().select(*on, *cols_left, *cols_right)
        return DataFrame(joined_df)

    def show(self, n: int = 20, truncate: bool = True) -> None:
        data = self._collected_data.head(n)
        print(data)

    def sort(
        self, *cols: str | Column | list[str | Column], ascending: bool | list[bool] | None = None
    ) -> "DataFrame":
        if len(cols) == 1 and isinstance(cols[0], list):
            cols = [f.col(col) for col in cols[0]]
        else:
            cols = [f.col(col) for col in cols]

        if ascending is None:
            ascending = [col.sort_ascending for col in cols]
        if isinstance(ascending, bool):
            ascending = [ascending] * len(cols)
        elif isinstance(ascending, list) and len(ascending) != len(cols):
            raise ValueError("Length of ascending list must match the number of columns")

        return DataFrame(
            self._lazy_data.sort(
                by=[c.expr for c in cols], descending=[not asc for asc in ascending]
            )
        )

    def orderBy(self, *cols: str | Column, ascending: bool | list[bool] = True) -> "DataFrame":
        return self.sort(*cols, ascending=ascending)
