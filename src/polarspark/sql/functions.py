import polars as pl

from polarspark.sql.column import Column, is_column


def col(col: str | Column) -> Column:
    return column(col)


def column(col: str | Column) -> Column:
    if is_column(col):
        return col
    return Column(pl.col(col))


def lit(col: Column | str | int | float | bool | list) -> Column:
    # ToDo: Test list
    if isinstance(col, Column):
        return col
    return Column(pl.lit(col))


def _get_base_col_name(col: Column) -> str | None:
    """Return the name of the column for a column created with col or column.

    Returns None if the column is a more complex column.
    """
    if str(col.expr).startswith('col("'):
        return str(col)[5:-2]  # Get just the name without the col()
    return None


def sum(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.sum()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"sum({col_name})")

    return Column(new_expr)


def max(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).max().alias(f"max({col})")

    if col_name := _get_base_col_name(col):
        return col.sum().alias(f"max({col_name})")

    return Column(col.expr.max())


def min(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).min().alias(f"min({col})")

    if col_name := _get_base_col_name(col):
        return col.min().alias(f"min({col_name})")

    return Column(col.expr.min())


def avg(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).mean().alias(f"avg({col})")

    if col_name := _get_base_col_name(col):
        return col.mean().alias(f"avg({col_name})")

    return Column(col.expr.mean())


def count(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).count().alias(f"count({col})")

    if col_name := _get_base_col_name(col):
        return col.count().alias(f"count({col_name})")

    return Column(col.expr.count())


def stddev(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).std().alias(f"stddev({col})")

    if col_name := _get_base_col_name(col):
        return col.std().alias(f"stddev({col_name})")

    return Column(col.expr.std())


def variance(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).var().alias(f"var_samp({col})")

    if col_name := _get_base_col_name(col):
        return col.var().alias(f"var_samp({col_name})")

    return Column(col.expr.var())


def asc(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).asc()
    return Column(col.expr).asc()


def desc(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).desc()
    return Column(col.expr).desc()
