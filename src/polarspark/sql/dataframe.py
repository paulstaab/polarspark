import polars as pl


class DataFrame:
    __data: pl.DataFrame

    def __init__(self, data: pl.DataFrame) -> None:
        self.__data = data

    def count(self) -> int:
        return self.__data.shape[0]

    @property
    def columns(self) -> list[str]:
        return self.__data.columns
