from polarspark.sql import SparkSession
from polarspark.sql.functions import col

def test_readme_example():
    """
    Test that the example from the README file works.
    """
    # Create a SparkSession
    spark = SparkSession.builder.getOrCreate()

    # Create a DataFrame
    data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
    df = spark.createDataFrame(data, schema=["id", "name"])

    # Select columns
    selected_df = df.select("id", "name")
    assert selected_df.columns == ["id", "name"]

    # Filter rows
    filtered_df = df.filter(col("id") > 1)
    assert filtered_df.count() == 2

    # Group by and aggregate
    grouped_df = df.groupby("id").count()
    assert grouped_df.count() == 3
