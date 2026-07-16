# CSV Sources

Use a local, remote, or globbed path ending in `.csv` or `.tsv`.

```text
data/raw/events_{{today:%Y-%m-%d}}.csv
```

Dataflow selects DuckDB's CSV reader based on the file extension.
