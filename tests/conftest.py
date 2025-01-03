from datetime import date, datetime

import pandas as pd
import pytest


@pytest.fixture
def test_data() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "a": [1, 2, 3, 1, 2, 3],
            "b": [2.0, 2.0, 3.0, 3.0, 4.0, 4.0],
            "c": [1, 1, 1, 2, 2, 2],
            "d": ["a", "b", "c", "d", "e", "f"],
            "e": [date(2023, 1, 1)] * 3 + [date.today()] * 3,
            "f": [datetime(2023, 1, 1, 13, 14, 15)] * 3 + [datetime.now()] * 3,
        }
    )
