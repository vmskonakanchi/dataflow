# Transformations

Transformations run in order between the source and sink. Built-in
transformations include select, filter, aggregate, and join. Dataflow also
supports registered Python transform plugins.

Built-in SQL steps are compiled and run by DuckDB. SQL execution remains
out-of-core; Python plugin execution has separate memory characteristics.

See [SQL transformations](../transformations/sql.md),
[filtering](../transformations/filtering.md),
[aggregations](../transformations/aggregations.md), and
[Python plugins](../transformations/python-plugins.md).
