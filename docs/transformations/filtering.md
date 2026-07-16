# Filtering

Use a filter transformation to apply a SQL `WHERE` condition.

```sql
status = 'active' AND event_date >= CURRENT_DATE - INTERVAL 7 DAY
```

The condition is evaluated by DuckDB.
