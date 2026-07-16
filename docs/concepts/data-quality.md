# Data Quality

Data-quality checks run after transformations and before the sink write. A
failed check prevents the output from being written.

Built-in checks:

- `not_null`
- `unique`
- `row_count_min`
- `accepted_values`
- custom SQL assertions

Use a row-count threshold and failure alerting to detect incomplete upstream
data early.
