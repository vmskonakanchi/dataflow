# Query Tool

The Query Tool runs ad-hoc DuckDB SQL against accessible data and returns
results over Apache Arrow IPC. Use it to inspect a source before building a
pipeline transformation.

Role-based data-access rules apply to S3 paths referenced by a query.
