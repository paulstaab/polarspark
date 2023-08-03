import pandas as pd

from tests import assert_df_transformation_equal


def test_persist(test_data: pd.DataFrame):
    assert_df_transformation_equal(test_data, lambda df, _: df.cache())
    assert_df_transformation_equal(test_data, lambda df, _: df.persist())
    assert_df_transformation_equal(test_data, lambda df, _: df.localCheckpoint())
