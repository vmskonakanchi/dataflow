# Contributing to Dataflow

Thanks for helping improve Dataflow. This guide covers local setup, the branch
and review workflow, database migrations, and the conventions we follow.

## Getting started

Requirements: Python 3.13+ and [uv](https://github.com/astral-sh/uv).

```bash
uv sync --all-extras          # base + dev (pytest) + ml (umap) + ai (llama-cpp)
uv run python main.py server --reload
```

- Base install is enough for the engine and web app. The `ml` extra is needed
  for the UMAP example plugin; the `ai` extra for the Flux assistant.
- Database migrations run automatically on startup. Use a throwaway database
  when experimenting: `DATAFLOW_DB=dev.db uv run python main.py server`.

## Project layout

```
src/
  api.py            FastAPI app, routes, auth middleware, RBAC enforcement
  config.py         SQLModel models + Pydantic config schemas + RBAC constants
  executor.py       DuckDB pipeline execution (SQL compile + write)
  auth_sso.py       OIDC/Entra SSO (claim -> role resolution, provisioning)
  settings.py       DB-backed runtime settings
  scheduler.py      APScheduler cron jobs
  transforms/       Python transform plugins (+ chunked execution)
  migrations/       Alembic migrations
  templates/        Jinja2 + HTMX + Alpine.js UI (no build step)
tests/              pytest suites
```

## Branch & review workflow

- `main` is the default branch. Prefer changes landing through a pull request
  rather than direct pushes to `main`.
- Branch per feature: `feat/<name>`, `fix/<name>`, or `docs/<name>` off `main`.
- Open a pull request into `main`; keep the title under ~70 characters and use
  the description for a summary, what was tested, and anything left out.
- Rebase or merge `main` in to resolve conflicts before requesting review.

## Commit messages

- Imperative subject line (`feat: add sink file sizing`), optionally with a
  `type:` prefix (`feat`, `fix`, `chore`, `docs`, `refactor`, `test`).
- Explain the *why* in the body when it isn't obvious.

## Database migrations (Alembic)

- Any change to a `table=True` SQLModel needs a migration. Generate one against
  a scratch DB, then review the generated SQL by hand (`alembic.ini` lives at the
  repo root; `DATAFLOW_DB_URL` points env.py at the target database):

  ```bash
  DATAFLOW_DB_URL="sqlite:////tmp/scratch.db" \
    uv run alembic revision --autogenerate -m "describe change"
  ```

- Set `down_revision` to the current head. If parallel branches produce
  **multiple heads**, unify them with a merge revision before merging to `main`:

  ```bash
  uv run alembic merge -m "merge heads" <head1> <head2> ...
  ```

- Verify a migration applies cleanly on a **fresh** database before pushing
  (never test against a shared/real config DB — it can leave it on a revision
  other branches don't know about).
- Keep `uv.lock` committed and in sync (`uv lock --check`). Never gitignore it.

## Tests

```bash
uv run pytest tests/ -q
```

- Add tests for new features and bug fixes. Set `DATAFLOW_DB` at the top of a
  test module (before importing app modules) to isolate its database, and use
  unique usernames across suites so files can run together in one process.
- Note: repeated in-process DuckDB executions can accumulate and segfault the
  interpreter. Prefer asserting authorization via route/permission checks over
  running many live queries; if a suite must run queries, keep them minimal or
  run that file in its own process.

## Adding a transform plugin

Drop a file into `src/transforms/<name>.py` (name matching `^[a-z][a-z0-9_]*$`)
exposing a single function:

```python
import pyarrow as pa

def transform(table: pa.Table, params: dict) -> pa.Table:
    return table
```

Plugins run arbitrary Python with the server's privileges — only add ones you
trust. The database only ever stores a plugin *name*, never code. For large
inputs, set `chunk_rows` on the step for bounded-memory execution.

## Security & secrets

- **Never commit secrets.** Enable secret scanning / push protection on the
  repository so a detected secret blocks the push. Avoid literal
  `password`/`secret` values even in tests — assign them to a variable instead.
- New network-exposed endpoints must enforce authentication and the appropriate
  RBAC permission. Flag any endpoint that intentionally skips auth in the PR.
- Report security-sensitive issues privately rather than in a public MR/issue.

## Code style

- Match the surrounding code; prefer clear names and small functions.
- Keep the UI dependency-free (Jinja2 + HTMX + Alpine.js, no npm/build step).
- Use parameterized queries and validate input; handle errors explicitly.
