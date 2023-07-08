from polarspark.sql.column import Column


def col(col: str) -> Column:
    return Column(col)


def column(col: str) -> Column:
    return Column(col)
