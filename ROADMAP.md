# Roadmap

> Track what's shipped and what's coming next.

---

## Core Engine

- [x] DuckDB execution (vectorized, out-of-core)
- [x] Auto-detect source format (Parquet, JSON, CSV)
- [x] `select`, `filter`, `aggregate`, `join` transform steps
- [x] Python transform plugins (drop-in `src/transforms/*.py` for ML/vector steps)
- [x] Memory-safe chunked plugin execution (`chunk_rows` — one isolated subprocess per row-slice)

## Storage

- [x] Parquet sink
- [x] Delta Lake sink (atomic, versioned writes)
- [x] Idempotent partition overwrites (no duplicate rows on retry)
- [x] Configurable output file sizing (`target_file_size` → multiple ~N MB files; optional `row_group_size`)
- [ ] Time-travel queries in the query tool (`VERSION AS OF N`)

## Data Quality

- [x] `not_null` check
- [x] `unique` check
- [x] `row_count_min` check
- [x] `accepted_values` check
- [x] Custom SQL assertion checks
- [x] Checks run before write — bad data never hits the sink
- [ ] Per-column data profiling (null %, distinct count, min/max)

## Reliability

- [x] Stage-level checkpointing (resume from last completed step)
- [x] Idempotent writes (safe to retry without duplicates)
- [x] Input validation (fail fast if source is missing or empty)
- [x] Graceful shutdown (SIGTERM/SIGINT saves checkpoint before exit)
- [x] Scheduler retry logic (configurable attempts + delay)
- [x] Durable SQLite-backed job queue (survives restarts, atomic claim)
- [x] Isolated worker process (a heavy pipeline can't take down the web server)
- [x] Crash reconciliation (jobs orphaned by a crash are auto-requeued)
- [x] Automatic retry with backoff
- [ ] Dead-letter holding state for permanently-failed jobs
- [ ] SLA deadlines (`must_complete_by` alerting)

## Alerting

- [x] Email alerts on pipeline failure
- [x] Email alerts on low row count threshold
- [x] Slack / webhook notifications
- [ ] Alert digest (batched summary instead of per-failure emails)

## Observability

- [x] Run history dashboard (status, duration, rows in/out)
- [x] Ad-hoc SQL query tool with Apache Arrow result streaming
- [ ] Per-stage execution timing in run logs
- [ ] Run comparison (diff metrics vs last successful run)
- [ ] Dashboard pipeline DAG visualization

## Security

- [x] Authentication (username/password)
- [x] RBAC — custom roles with a permission matrix (create/edit/delete/run per resource), server-side enforced on every route; seeded viewer/editor/admin
- [x] Data-access scoping — per-role S3 bucket allow/deny lists, enforced on the query tool and pipeline paths; pipeline `run_as` role with anti-escalation (app-level; AWS AssumeRole next)
- [ ] Secret references in config (`${secrets.KEY}`)
- [x] Audit log — who did what (create/edit/delete/run pipelines & cronjobs, user/role/settings changes, logins) with user, role, target, detail, IP; admin viewer with filters (audit.view); configurable retention + structured JSON to stdout for log shipping
- [x] SSO (OIDC / Microsoft Entra) with group → role mapping — additive to local login; auto-provisions users, refreshes role from IdP groups on each sign-in
- [ ] Per-user AWS credential scoping (`AssumeRole` by identity) for data-level isolation

## Local AI Chat

- [x] Sidebar chatbox UI for AI-assisted query writing
- [x] Runs entirely on local CPU — no cloud API calls, fully offline
- [x] Powered by llama.cpp via llama-cpp-python (pre-built CPU wheels, zero compile)
- [x] User-selectable models (Google Gemma 4 E4B, Qwen3 4B Text-to-SQL, Phi-3 Mini, Qwen2 1.5B)
- [x] Context-aware: auto-detects table schemas, column names, and types from data/ directory
- [x] Generates DuckDB-compatible SQL from natural language questions
- [x] Insert generated query directly into the SQL query tool with one click
- [x] Model download manager (pulls GGUF models from HuggingFace on first use)
- [ ] Configurable inference settings UI (temperature, max tokens, threads)
- [ ] Conversation memory (multi-turn context within a session)
- [ ] Custom model support (bring your own GGUF file)
