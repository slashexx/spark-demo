# Dashboard time-filter fix

The original panels queried pre-aggregated summary tables (`summary`,
`daily_counts`, `mag_buckets`, `top_strongest`, `top_places`) that each
contained a single snapshot, so Grafana's time-range picker had no effect
on what was shown. The fix rewrites every panel to query the raw `events`
table and bound it by the picker via Grafana's built-in `$__from` / `$__to`
epoch-millisecond interpolations —

```sql
time_utc >= datetime($__from/1000, 'unixepoch')
   AND    time_utc <= datetime($__to/1000, 'unixepoch')
```

The `frser-sqlite-datasource` plugin does not implement the
`$__timeFilter()` / `$__unixEpochFrom()` macros (they return a
*missing named argument* error), but Grafana core substitutes `$__from` /
`$__to` before the query reaches the plugin, which works cleanly.

### Verified behaviour (tested via `/api/ds/query`)

| Time range | `COUNT(*) FROM events` |
|---|---|
| now-30d → now | 11,423 |
| now-7d  → now |  2,468 |
| now-24h → now |    291 |
| now-1h  → now |      0 |

Counts scale correctly with the range, proving the macro interpolation
is reaching SQLite.
