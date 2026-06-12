---
name: new-asset
description: Add a new Dagster asset or dbt model (staging or mart) following the bronze->staging->marts layering, with tests, asset checks, and partitioning. Use when adding any new dataset, aggregate, segment, or trend table.
argument-hint: <asset-name> [staging|mart]
---

# New asset / model

Follow @.claude/memory/standards.md layering. Confirm only what's missing: name,
layer, upstream dependencies, grain (one row per ...?), partitioning (daily?).

## Steps
1. Plan: where it sits in bronze->staging->marts, upstream assets, incremental strategy.
   Show plan; wait for go-ahead.
2. dbt model (if SQL): incremental with explicit `unique_key`; schema.yml with
   not_null/unique tests; document the grain in the model description.
3. Dagster: declare as asset with partitions matching upstream; add asset checks —
   freshness (max event ts lag), row volume vs trailing avg, null-rate on key columns.
4. ClickHouse target: correct engine (ReplacingMergeTree for deduped staging,
   Summing/AggregatingMergeTree for additive marts), ORDER BY = query pattern,
   PARTITION BY toYYYYMM unless justified.
5. Tests: pytest for any Python transform; `dbt build --select <model>` green.
6. Update `_state.md`.

## Constraints
- No full-history rebuilds: incremental or partitioned backfill only.
- Backfills run via the Dagster partitioned job over Iceberg, never ad-hoc SQL.
