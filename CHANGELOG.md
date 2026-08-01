# Changelog

All notable changes to Dataflow are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] — 2026-07-28

### Added

**Connector SDK**
- `Connector` abstract base class with a unified `read()` / `write()` / `test_connection()` / `metadata()` interface.
- Auto-discovery registry: drop a Python file into `src/connectors/` and it registers itself automatically.
- `FileConnector`: pluggable source/sink for local files, S3, GCS, and HTTP file endpoints (Parquet, Delta, JSON, CSV).
- `HttpConnector`: manages DuckDB HTTP secrets supporting bearer-token, HTTP basic, and custom header authentication.

**Pipeline Integration**
- `PipelineConfig` extended with backward-compatible `source_type` and `source_config` fields.
- Executor delegates source setup to `connector.read()` at runtime.
- Saved connectors are resolved from the database at execution time — no hard-coded paths needed.
- Sink extension validation: rejects `.csv` / `.json` outputs (Parquet/Delta only).

**Database**
- New `SavedConnector` table (`name`, `type`, config JSON, timestamps).
- `Pipeline` table: `source_path` is now nullable; `source_type` and `source_config` columns added.
- Alembic migration with automatic backfill for existing pipelines.

**UI**
- New **Connectors** page: list, create, edit, test connection, and delete saved connectors.
- Pipeline wizard restructured into six tabs: Basics, Source & Sink, Execution, Transforms, Checks, Alerts.
- Source tab: type dropdown + connector picker (file type allows an inline path override).
- Sink tab: connector picker with path auto-fill.
- Guard prevents using the same connector as both source and sink.
- Sidebar navigation link with plug icon for the Connectors page.

## [0.1.0] — 2026-07-17

First consolidated release of Dataflow: the core DuckDB data pipeline engine, web dashboard, and enterprise features.

### Added

**Core Engine & Dashboard**
- Pipelines mapping local/S3 sources (Parquet/JSON/CSV) to a Parquet file or Delta Lake table.
- SQL transform compiler (select/filter/aggregate/join compiled into out-of-core DuckDB query).
- Python transform plugin system (drop a `transform(table, params)` file into `src/transforms/`).
- Built-in data-quality checks, cron scheduling with retries, email/webhook alerts, and a dark-mode web dashboard.
- Pipeline template variables for `today`, `yesterday`, and `now`, with Python `strftime` formatting.
- Per-pipeline IANA timezone support for template resolution and cron scheduling.
- Global loading overlay for query execution, exports, and HTMX requests.
- Ad-hoc DuckDB query tool inside the web UI.

**Access Control & Auditing**
- Role-based access control (RBAC): custom roles defined through a permission matrix, enforced server-side.
- Per-role S3 data-access scoping: `bucket_allow` / `bucket_deny` lists enforced on query tool and pipeline paths.
- Audit log of security-relevant actions with user, role, target, detail, and IP. Emitted to stdout in JSON and viewable in-app.

**Identity**
- Single sign-on (OIDC / Microsoft Entra ID), additive to local username/password login. Auto-provisions users and maps IdP groups to RBAC roles.

**Performance & Operations**
- Memory-safe chunked plugin execution (`chunk_rows`): streams plugin input through subprocesses.
- Configurable sink output file sizing (`target_file_size` and `row_group_size`).
- Comprehensive test suite covering config schemas, settings database, executor, job queue, worker, plugins, and AI assistant.
- Structured documentation under `docs/`.

### Changed
- Fixed syntax parser bugs in the pipeline executor's SQL JOIN compiler.
- Relicensed under the Apache License 2.0.

### Security
- Session cookies signed with an auto-generated secret; passwords hashed with bcrypt.

[Unreleased]: https://github.com/vmskonakanchi/dataflow/compare/v0.2.0...feat/connectors
[0.2.0]: https://github.com/vmskonakanchi/dataflow/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/vmskonakanchi/dataflow/releases/tag/v0.1.0
