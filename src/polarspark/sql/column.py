from typing import Self

import polars


class Column(polars.Expr):
    def __init__(self, column: polars.Expr | str) -> None:
        if isinstance(column, str):
            column = polars.col(column)
        self.__col = column

    def alias(self, alias: str) -> Self:
        self.__col = self.__col.alias(alias)
        return self

    def to_polars(self) -> polars.Expr:
        return self.__col

    def __add__(self, other) -> Self:
        return Column(self.__col + other)
