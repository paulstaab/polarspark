import polars as pl

from polarspark.sql.column import Column


def col(col: str) -> Column:
    return pl.col(col)


def column(col: str) -> Column:
    return pl.col(col)


def sum(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).sum().alias(f"sum({col})")

    if str(col).startswith('col("'):
        col_name = str(col)[5:-2]  # Get just the name without the col()
        return col.sum().alias(f"sum({col_name})")

    return col.sum()


def max(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).max().alias(f"max({col})")

    if str(col).startswith('col("'):
        col_name = str(col)[5:-2]  # Get just the name without the col()
        return col.max().alias(f"max({col_name})")

    return col.max()
