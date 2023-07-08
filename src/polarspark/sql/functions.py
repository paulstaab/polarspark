import polars as pl

from polarspark.sql.column import Column


def col(col: str) -> Column:
    return pl.col(col)


def column(col: str) -> Column:
    return pl.col(col)
