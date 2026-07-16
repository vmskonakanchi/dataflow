# Template Variables

Template variables are resolved once, before pipeline execution. Sources,
joins, and sinks receive resolved strings and do not need template-specific
logic.

Supported variables:

| Variable | Default output |
| --- | --- |
| `{{today}}` | `YYYY-MM-DD` |
| `{{yesterday}}` | `YYYY-MM-DD` |
| `{{now}}` | `YYYY-MM-DDTHH:MM:SS` |

Use Python `strftime` directives for formatting:

```text
https://example.com/sales_{{today:%Y_%m_%d}}.parquet
/output/result_{{yesterday:%Y-%m-%d}}.parquet
{{now:%H:%M:%S}}
```

Only `today`, `yesterday`, and `now` are supported. Expressions,
environment variables, nesting, and user-defined variables are rejected.
