import polars as pl

from polarspark.sql.column import Column


def col(col: str) -> Column:
    return pl.col(col)


def column(col: str) -> Column:
    return pl.col(col)


def sum(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).sum().alias(f"sum({col})")

    return col.sum()


def max(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).max().alias(f"max({col})")

    return col.max()
