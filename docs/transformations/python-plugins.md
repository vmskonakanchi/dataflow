# Python Transform Plugins

Use a Python plugin when a transformation cannot be expressed in SQL. Plugins
are discovered from `src/transforms/` and appear in the pipeline editor.

## Plugin contract

Create `src/transforms/<name>.py`, where `<name>` matches
`^[a-z][a-z0-9_]*$`.

```python
import pyarrow as pa


def transform(table: pa.Table, params: dict) -> pa.Table:
    return table
```

The function receives the current `pyarrow.Table` and the JSON parameters
configured on the pipeline step. It must return a `pyarrow.Table`.

```json
{
  "type": "python",
  "function": "my_plugin",
  "params": {"column": "price"}
}
```

New plugin files are discovered without restarting Dataflow. Restart after
editing an already imported plugin, because Python caches imported modules.

## Memory and chunking

SQL transformations stream through DuckDB. A Python plugin normally
materializes its input in memory, so its full working set must fit in RAM.

Set `chunk_rows` to process a plugin in bounded row slices:

```json
{
  "type": "python",
  "function": "my_heavy_plugin",
  "params": {"threshold": 0.7},
  "chunk_rows": 12000
}
```

Each slice runs in a fresh subprocess. All slices must return the same schema.
`chunk_rows: 0` is the default in-process behavior.

## Security

Plugins execute arbitrary Python with the server's privileges. Only install
reviewed plugin files and treat them as trusted application code.
