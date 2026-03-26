# Dataflow

Dataflow is a config-driven data pipeline platform for small and mid-sized engineering teams. It allows you to define data sources, sinks, pipelines, and cron-based schedules using simple JSON configuration files. Built with extensibility in mind, it uses native database drivers and a registry-based pattern for connectors and transforms.

## Installation

1. Clone the repository and navigate to the project directory.
2. Install the dependencies and create a virtual environment using `uv`:
```bash
uv sync
```

## Quick Start

1. **Validate your configuration**:
```bash
uv run dataflow validate --config-dir ./configs
```

2. **Run a pipeline manually**:
```bash
uv run dataflow run --pipeline orders_daily_sync --config-dir ./configs
```

3. **Start the scheduler**:
```bash
uv run dataflow schedule --config-dir ./configs
```

## Running the Dashboard

To see the visual dashboard and manually trigger pipelines:

1. **Start the API Backend**:
```bash
uv run dataflow-api
```
*(Alternatively: `uv run fastapi dev dataflow/api/main.py`)*

2. **Start the UI Frontend**:
```bash
cd ui
npm install
npm run dev
```
Visit `http://localhost:5173` to view the dashboard.

## Config File Reference

### sources.json
Defines the data sources. Supported types: `postgres`, `mysql`, `csv`, `rest_api`.

Example (`postgres`):
```json
{
  "name": "pg_sales_db",
  "type": "postgres",
  "host": "localhost",
  "port": 5432,
  "database": "sales_db",
  "username": "user",
  "password": "pass"
}
```

## CLI Command Reference

- `validate`: Loads and validates all 4 config files (sources, sinks, pipelines, cronjobs).
- `run --pipeline NAME`: Executes a single pipeline end-to-end.
- `schedule`: Starts the APScheduler to run enabled cronjobs.
- `history --pipeline NAME`: Displays the run history for a specific pipeline from the SQLite log.
- `list-pipelines`: Lists all pipelines, their sources, sinks, and scheduled cronjobs.

## Environment Variables for SMTP Alerts

To enable email alerts on pipeline failure or low row counts, set the following environment variables:

- `DATAFLOW_SMTP_HOST`: e.g., `smtp.gmail.com`
- `DATAFLOW_SMTP_PORT`: e.g., `587`
- `DATAFLOW_SMTP_USERNAME`: Your SMTP username
- `DATAFLOW_SMTP_PASSWORD`: Your SMTP password
- `DATAFLOW_SMTP_FROM`: The sender email address

## How to Add a New Connector

1. Create a new file in `dataflow/connectors/[name].py`.
2. Inherit from `BaseConnector`.
3. Implement `extract()` and `load()` methods.
4. Register the connector using `@BaseConnector.register("type_name")`.
5. Import your new file in `dataflow/executor/pipeline_runner.py` to ensure registration.

## How to Add a New Transform

1. Create a new file in `dataflow/transforms/[name].py`.
2. Inherit from `BaseTransform`.
3. Implement the `apply()` method.
4. Register the transform using `@BaseTransform.register("type_name")`.
5. Add the new transform type to `PipelineConfig` in `dataflow/config/models.py`.
6. Import your new file in `dataflow/executor/pipeline_runner.py` and handle its instantiation in the transform loop.