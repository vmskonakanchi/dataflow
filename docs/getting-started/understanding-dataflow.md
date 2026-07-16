# Understanding Dataflow

Dataflow is a self-hosted data pipeline engine built around DuckDB.

```text
Pipeline definition -> worker job -> template resolution -> DuckDB execution -> sink
```

A pipeline reads one source, applies ordered transformations and optional
quality checks, then writes one sink. Pipeline definitions are stored in
Dataflow's configuration database and run independently from the web server.
