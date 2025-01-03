import polars as pl

from polarspark.sql.column import Column, is_column


def col(col: str) -> Column:
    return pl.col(col)


def column(col: str) -> Column:
    return pl.col(col)


def lit(col: Column | str | int | float | bool | list) -> Column:
    # ToDo: Test list
    if is_column(col):
        return col
    return pl.lit(col)


def _get_base_col_name(col: Column) -> str | None:
    """Return the name of the column for a column created with col or column.

    Returns None if the column is a more complex column.
    """
    if str(col).startswith('col("'):
        return str(col)[5:-2]  # Get just the name without the col()
    return None


def sum(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).sum().alias(f"sum({col})")

    if col_name := _get_base_col_name(col):
        return col.sum().alias(f"sum({col_name})")

    return col.sum()


def max(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).max().alias(f"max({col})")

    if col_name := _get_base_col_name(col):
        return col.sum().alias(f"max({col_name})")

    return col.max()


def min(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).min().alias(f"min({col})")

    if col_name := _get_base_col_name(col):
        return col.min().alias(f"min({col_name})")

    return col.min()


def avg(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).mean().alias(f"avg({col})")

    if col_name := _get_base_col_name(col):
        return col.mean().alias(f"avg({col_name})")

    return col.mean()


def count(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).count().alias(f"count({col})")

    if col_name := _get_base_col_name(col):
        return col.count().alias(f"count({col_name})")

    return col.count()


def stddev(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).std().alias(f"stddev({col})")

    if col_name := _get_base_col_name(col):
        return col.std().alias(f"stddev({col_name})")

    return col.std()


def variance(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).var().alias(f"variance({col})")

    if col_name := _get_base_col_name(col):
        return col.var().alias(f"variance({col_name})")

    return col.var()
