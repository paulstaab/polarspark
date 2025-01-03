import polars as pl
from polars.lazyframe.group_by import LazyGroupBy

from polarspark.sql import functions
from polarspark.sql.column import Column
from polarspark.sql.dataframe import DataFrame


class GroupedData:
    def __init__(self, group_by: LazyGroupBy) -> None:
        self._group_by = group_by

    def count(self) -> DataFrame:
        return DataFrame(self._group_by.count())

    def mean(self, *cols: str) -> DataFrame:
        return DataFrame(self._group_by.agg(*[pl.mean(c).alias(f"avg({c})") for c in cols]))

    def avg(self, *cols: str) -> DataFrame:
        return self.mean(*cols)

    def min(self, *cols: str) -> DataFrame:
        return DataFrame(self._group_by.agg(*[pl.min(c).alias(f"min({c})") for c in cols]))

    @staticmethod
    def _align_agg_col_name(col: Column) -> Column:
        """Give the column the name that will be generated by PySpark.

        After aggregration, PySpark and polars generate different names for columns if they do not
        have an explicit name set with .alias().

        It will be difficult to fully resolve this. For now, we align only the simple (and most
        common) case of a selected columns without modificions, e.g. generated with f.col("xzy").
        For more complicated columns, we raise a NotImplementedError.
        """
        if not isinstance(col, Column):
            raise ValueError(f"Invalid value {col}, expected a Column.")

        # Now it gets hacky... detect if a alias was set
        if str(col).split(".")[-1].startswith("alias("):
            return col

        raise NotImplementedError(
            f"""
            Aggregating a column with without giving it an explict name with .alias() will
            generate a column with a different name than PySpark and is not supported
            at the moment. Please explictly set a name for the result for the aggregration
            {col} with .alias().
            """
        )

    def agg(self, *exprs: Column | dict[str, str]):
        if isinstance(exprs[0], dict):
            columns = [getattr(functions, agg)(col_name) for col_name, agg in exprs[0].items()]
        else:
            columns = list(exprs)

        return DataFrame(self._group_by.agg(*[self._align_agg_col_name(c) for c in columns]))
