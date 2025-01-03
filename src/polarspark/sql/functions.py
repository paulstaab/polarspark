import polars as pl

from polarspark.sql.column import Column


def col(col: str | Column) -> Column:
    return column(col)


def column(col: str | Column) -> Column:
    if isinstance(col, str):
        col = Column(pl.col(col))
    return col


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
        return str(col.expr)[5:-2]  # Get just the name without the col()
    return None


def sum(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.sum()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"sum({col_name})")

    return Column(new_expr)


def max(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.max()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"max({col_name})")

    return Column(new_expr)


def min(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.min()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"min({col_name})")

    return Column(new_expr)


def avg(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.mean()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"avg({col_name})")

    return Column(new_expr)


def count(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.count()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"count({col_name})")

    return Column(new_expr)


def stddev(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.std()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"stddev({col_name})")

    return Column(new_expr)


def variance(col: str | Column) -> Column:
    col = column(col)

    new_expr = col.expr.var()

    if col_name := _get_base_col_name(col):
        new_expr = new_expr.alias(f"var_samp({col_name})")

    return Column(new_expr)


def asc(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).asc()
    return Column(col.expr).asc()


def desc(col: str | Column) -> Column:
    if isinstance(col, str):
        return column(col).desc()
    return Column(col.expr).desc()
