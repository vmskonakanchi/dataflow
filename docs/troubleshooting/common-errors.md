# Common Errors

## Source path matched no files

Verify the resolved source path exists and that glob patterns match at least one
file. Check the pipeline timezone if the path uses date template variables.

## Unsupported template variable

Only `today`, `yesterday`, and `now` are supported.

## Access denied for run_as role

Ensure the source, join, and sink paths are allowed by the pipeline's
`run_as` role.
