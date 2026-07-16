# Pipeline Timezones

Every pipeline has an IANA timezone, selected in the pipeline editor. It
defaults to `UTC`.

The pipeline timezone affects only template resolution:

```text
Pipeline timezone: Asia/Kolkata
Execution instant: 2026-07-16 20:15 UTC
{{today}}: 2026-07-17
```

It does not change source or sink behavior. For daily pipelines, use the same
timezone on the pipeline and its schedule unless cross-timezone processing is
intentional.
