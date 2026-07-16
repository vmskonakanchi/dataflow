# HTTPS Sources

Use a direct HTTPS URL as a source. DuckDB reads the remote object through its
HTTP filesystem support.

```text
https://example.com/datasets/sales_{{today:%Y_%m_%d}}.parquet
```

The date variable is resolved by the pipeline before DuckDB reads the URL.
