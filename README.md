# polarspark

Experimental, partial implementation of PySpark's batch Dataframe API in pola.rs.

## Goal
This package aims to make it easy to answer the questions:

- Will my existing PySpark application run in pola.rs?
- Will it fit into memory?
- Will I gain performance or cost savings when converting it?

To do this, it aims to provide a quick-and-dirty method for running the application using pola.rs
by redicting import from `pyspark` to `polarspark` and possibly adjusting/commenting a couple of
things.

**Do not use this anyway near your production environment**.
Write a proper re-implementation in pola.rs if you like the results.

## Implementation Status

| Feature | Status | Comment |
| ------- | ------ | ------- |
| DataFrame.select           | Done |
| DataFrame.filter           | Done |
| DataFrame.groupby().agg()  | Done | Only a few aggregration functions are implemented at the moment  |
| DataFrame.withColumn       | Done |
| DataFrame.join             | Partially | Only string argumnets for on are supported |
| DataFrame.persist          | Partially | Caching to disk is not supported |
| DataFrame.toPandas         | Done |
| DataFrame.sort             | Done |
| DataFrame.orderBy          | Done |


## Limitations
- polarspark only supports the batch dataframe API. No Structured Streaming, MLlib, etc.
- Everything too fancy will likely not work: spark plugins, extra jars, ...
- In DF.join(), the join column need to be passed as strings. PySparks df1.col_a == df2.col_b
  syntax is not supported.
- polars dataframe can not have two columns with the same name, while PySpark dataframes can.
  If your applications uses such dataframes (it shouldn't!), polarspark will not work.
- If columns are aggregrated without an alias, i.e.
  when using ´df.agg(f.sum("x"))´ instead of ´df.agg(f.sum("x").alias("sum_x"))´,
  PySpark and polars assign different names to the generated column.
  polarspark will try to set the PySpark names in simple cases like the one above,
  but will fail with a `NotImplementedError` for more complex cases like
  ´df.agg(f.sum(f.col("x") + 1))´. You need to set an alias in this case.

## Installation

To install the package and its dependencies, clone the repo and run the following command:

```bash
pip install .
```

## Usage

Here are some examples of how to use the main features of the project:

```python
from polarspark.sql import SparkSession
from polarspark.sql.functions import col, asc, desc

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Create a DataFrame
data = [(1, "Alice"), (2, "Bob"), (3, "Cathy")]
df = spark.createDataFrame(data, schema=["id", "name"])

# Select columns
df.select("id", "name").show()

# Filter rows
df.filter(col("id") > 1).show()

# Group by and aggregate
df.groupby("id").count().show()

# Sort rows
df.sort(asc("id")).show()
df.sort(df.id.desc()).show()
```

## Contribution Guidelines

We welcome contributions to the project! To contribute, please follow these steps:

1. Fork the repository
2. Create a new branch for your feature or bugfix
3. Make your changes
4. Submit a pull request with a clear description of your changes

## Testing

This project uses `pytest` as the testing framework. To run the tests, use the following command:

```bash
pytest
```

## License

This project is licensed under the Apache License 2.0. See the [LICENSE](LICENSE) file for more information.
