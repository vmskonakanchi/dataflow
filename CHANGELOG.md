# Changelog

All notable changes to Dataflow are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] — 2026-07-11

First consolidated release: the core engine plus the enterprise access, auditing,
performance, and identity features.

### Added

**Access control**
- Role-based access control (RBAC): custom roles defined through a permission
  matrix (`create`/`edit`/`delete`/`run` per resource, plus query, AI, and
  admin permissions), enforced server-side on every route. Only `admin` is
  seeded by default.
- Per-role S3 **data-access scoping**: `bucket_allow` / `bucket_deny` lists
  enforced on the query tool and pipeline paths. Empty allow-list = unrestricted
  (opt-in); admin/wildcard bypasses.
- Pipeline `run_as` role with anti-escalation (only admins may assign a role
  other than their own).

**Identity**
- Single sign-on (OIDC / Microsoft **Entra ID**), additive to local
  username/password login — local accounts always remain a fallback.
  Auto-provisions users on first sign-in and maps IdP groups to RBAC roles,
  refreshing the role from the IdP on every login.

**Auditing**
- Audit log of security-relevant actions (logins; pipeline/cronjob create,
  edit, delete, run; user/role/settings changes) with user, role, target,
  detail, and IP. In-app viewer with filters (gated on `audit.view`),
  configurable retention, and a structured JSON copy emitted to stdout for
  SIEM/log-shipping.

**Performance**
- Configurable sink output file sizing: a Parquet sink with `target_file_size`
  (e.g. `200MB`) writes multiple size-bounded files into a directory; optional
  `row_group_size` tuning. Unset = single file (unchanged behavior).
- Memory-safe **chunked plugin execution** (`chunk_rows`): streams a plugin's
  input through bounded row-slices, each in a fresh subprocess, so peak memory
  stays bounded regardless of dataset size.

**Core engine** (baseline)
- Pipelines mapping local/S3 sources (Parquet/JSON/CSV) to a Parquet file or
  Delta Lake table, with a SQL transform compiler (select/filter/aggregate/join
  compiled into a single out-of-core DuckDB query).
- Python transform plugin system (drop a `transform(table, params)` file into
  `src/transforms/`).
- Built-in data-quality checks, cron scheduling with retries, alerting
  (email + webhook), and a dark-mode web dashboard with an ad-hoc query tool.

### Changed
- Relicensed under the Apache License 2.0.

### Security
- Session cookies signed with an auto-generated secret; passwords hashed with
  bcrypt. SSO users receive an unusable password hash so they can't
  password-login.
- All administrative and data routes enforce explicit permissions server-side.

[Unreleased]: https://github.com/vmskonakanchi/dataflow/compare/v0.1.0...main
[0.1.0]: https://github.com/vmskonakanchi/dataflow/releases/tag/v0.1.0
