# Production

For production, configure authentication, role-based access control, storage
permissions, backups, monitoring, and log collection.

Dataflow's reliability features include a durable SQLite-backed job queue,
separate worker process, checkpointing, retries, and crash reconciliation.

| Concern | Dataflow behavior |
| --- | --- |
| Missing source | Fails before execution when a local source glob matches no files. |
| Worker crash | Stale jobs are reconciled and retried according to job policy. |
| Transform failure | Checkpoints can resume completed stages. |
| Bad output | Data-quality checks run before the sink write. |
| Remote storage | S3-compatible paths use DuckDB extensions and configured credentials. |

Use persistent volumes for the configuration database and data directories.
Restrict production access with roles and bucket allow/deny rules. Collect
application logs and back up the configuration database before upgrades.
