from typing import Any
from polars import Expr as Column  # noqa


def is_column(obj: Any) -> bool:
    return isinstance(obj, Column)
