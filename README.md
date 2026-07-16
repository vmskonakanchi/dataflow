# Dataflow

> A self-hosted data pipeline engine for moving and transforming files with DuckDB.

![Dataflow dashboard](./assets/demo.gif)

Dataflow gives teams a focused control plane for file-based data pipelines:
define a source, transform it with SQL or Python, validate the result, and
write Parquet or Delta Lake output. It runs as a single self-hosted service
with a web UI, durable jobs, and scheduled execution.

## Why Dataflow

- **Simple to operate**: FastAPI, DuckDB, SQLite, and no frontend build step.
- **Built for files**: local paths, S3-compatible storage, HTTPS, CSV, JSON,
  and Parquet inputs.
- **Reliable by default**: durable job queue, retries, checkpoints, worker
  isolation, and data-quality checks before writes.
- **Flexible execution**: SQL transformations, Python plugins, and an ad-hoc
  DuckDB query tool.
- **Self-hosted**: keep data, configuration, and authentication under your
  control.

## Quick Start

```bash
git clone https://github.com/vmskonakanchi/dataflow.git
cd dataflow
uv sync
uv run python main.py server --reload
```

Open `http://localhost:8000` and complete the initial administrator setup.

For Docker, production setup, and your first pipeline, start with the
[documentation](./docs/README.md).

## Documentation

- [Getting Started](./docs/getting-started/installation.md)
- [Core Concepts](./docs/concepts/pipelines.md)
- [Template Variables and Timezones](./docs/concepts/template-variables.md)
- [Scheduling](./docs/concepts/scheduling.md)
- [Sources](./docs/sources/)
- [Transformations](./docs/transformations/)
- [Sinks](./docs/sinks/)
- [Examples](./docs/examples/)
- [Enterprise Operations](./docs/enterprise/)
- [Troubleshooting](./docs/troubleshooting/)

## Contributing

Contributions are welcome. Read [CONTRIBUTING.md](./CONTRIBUTING.md) for local
development, testing, migrations, and project conventions.

## Project Information

- [Changelog](./CHANGELOG.md)
- [Apache License 2.0](./LICENSE)
- [Notice](./NOTICE)
