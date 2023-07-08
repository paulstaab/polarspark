import pandas as pd

from tests import assert_df_transformation_equal


def test_persist():
    # given
    data = pd.DataFrame({"name": ["Alice", "Bob"], "age": [22, 31]})
    # then
    assert_df_transformation_equal(data, lambda df, _: df.cache())
    assert_df_transformation_equal(data, lambda df, _: df.persist())
    assert_df_transformation_equal(data, lambda df, _: df.localCheckpoint())
