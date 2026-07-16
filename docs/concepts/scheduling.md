# Scheduling

Schedules use five-part cron expressions and an IANA timezone. The schedule
timezone controls **when** a job is enqueued; the pipeline timezone controls
how template variables resolve when that job starts.

```text
Cron: 0 0 * * *
Schedule timezone: Asia/Kolkata
Pipeline timezone: Asia/Kolkata
```

This runs at midnight in India and resolves `{{today}}` in that same timezone.
