# Parquet Sinks

Use the Parquet sink for a single output file, or configure a target file size
to write multiple Parquet files into a directory.

```text
/output/sales_{{today:%Y-%m-%d}}.parquet
```

Dataflow overwrites the target for each successful run.
