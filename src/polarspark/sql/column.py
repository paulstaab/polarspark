from typing import Any

from polars import Expr


class Column:
    sort_ascending: bool = True

    def __init__(self, expr: Expr):
        self.expr = expr

    def asc(self) -> "Column":
        self.sort_ascending = True
        return self

    def desc(self) -> "Column":
        self.sort_ascending = False
        return self


def is_column(obj: Any) -> bool:
    return isinstance(obj, Column)
