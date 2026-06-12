---
name: flink-job
description: Create or modify the PyFlink streaming job (trend detection, sessionization, or other stateful event-time computation). Use for anything involving Flink, windows, watermarks, keyed state, or checkpoints.
argument-hint: <job-name>
---

# Flink job (v3 only — check current version in tech-stack.md first)

If we're still in v1/v2, say so and ask whether to proceed anyway.

## Design checklist (resolve before writing code)
1. Why Flink? State explicitly what this needs that a ClickHouse MV can't do
   (event-time watermarks? keyed baseline state? stream-stream join?). If nothing — stop,
   recommend an MV instead.
2. Windows: type (sliding/tumbling/session), size, allowed lateness. Watermark bound
   from @.claude/memory/standards.md.
3. State: what's keyed by what, expected cardinality, TTL.
4. Output contract: Avro schema of the result topic / ClickHouse sink, batching config.

## Implementation rules
- One job = one responsibility; new computation = new job module.
- Exactly-once checkpoints to MinIO `flink-checkpoints/`; uid() set on every stateful
  operator so state survives job upgrades.
- Late/malformed events to side output -> DLQ topic, with a counter metric.
- Expose metrics: records in/out, watermark lag, checkpoint duration.
- Local test: bounded source from fixture file through the same pipeline topology.

## Deployment note
Job image is built by CI here; the FlinkDeployment CRD lives in homelab-ops.
After changing parallelism/state shape, note the upgrade path (savepoint required?) in
the PR description and in `_state.md`.
