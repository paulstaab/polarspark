import polars as pl
from polars.lazyframe.groupby import LazyGroupBy

from polarspark.sql.dataframe import DataFrame
from polarspark.sql.column import Column


class GroupedData:
    def __init__(self, group_by: LazyGroupBy) -> None:
        self._group_by = group_by

    def count(self) -> DataFrame:
        return DataFrame(self._group_by.count())

    def mean(self, *cols: str) -> DataFrame:
        return DataFrame(self._group_by.agg(*[pl.avg(c).alias(f"avg({c})") for c in cols]))

    def avg(self, *cols: str) -> DataFrame:
        return self.mean(*cols)

    def agg(self, *exprs: Column | dict[str, str]):
        if isinstance(exprs[0], dict):
            return DataFrame(self._group_by.agg(**exprs[0]))
        return DataFrame(self._group_by.agg(*exprs))
