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
