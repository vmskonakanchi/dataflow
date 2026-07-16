# FAQ

## Does Dataflow support dynamic dates in URLs?

Yes. Use a pipeline template variable, such as
`https://example.com/data_{{today:%Y_%m_%d}}.parquet`.

## Which timezone does a scheduled pipeline use?

The schedule timezone controls when it fires. The pipeline timezone controls
template variable resolution.

## Does Dataflow support database sources?

Not yet. See [Database Sources](../sources/database.md).
