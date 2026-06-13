# Step A — verify notes (v1 ingest, local end-to-end)

_Stub for you to fill in as you run the VERIFY.md runbook by hand._
_Date run: ____  ·  Stack: docker-compose.dev.yml on branch feat/v1-local-stack_

Runbook: `../../../VERIFY.md`. Infra was left running from this session and the slate was
reset (empty cursor, fresh topics). `make up` is idempotent if you need to restart it.

---

## (a) Events flowing into bsky.posts.v1

Commands run: `make run-ingest` (terminal 1) + `rpk topic consume bsky.posts.v1 ... -n 5`.

- [ ] `jetstream_connected` appeared in the logs
- [ ] `ingest_stats` shows `events_total` climbing, `last_event_age_s` ≈ 0, `dlq_total: 0`
- [ ] 5 Avro-framed messages consumed from bsky.posts.v1

Observations:
>

---

## (b1) Graceful stop — NO duplicates

- Cursor before Ctrl-C (C1): `__________`
- Cursor after Ctrl-C (C2, should be > C1): `__________`
- [ ] On restart, logs show `resume_cursor=<C2>` / `jetstream_connected cursor=<C2>`
- [ ] No duplicate overlap observed

Observations:
>

## (b2) Hard crash (SIGKILL) — duplicate overlap

Kill: `docker compose -f docker-compose.dev.yml kill -s SIGKILL ingest` (from a 2nd terminal).

- Cursor after SIGKILL (C3, last periodic checkpoint): `__________`
- [ ] On restart, logs show `resume_cursor=<C3>`
- [ ] Replay overlap observed (events between C3 and the kill re-produced), bounded by the
      100-ack / 2s checkpoint cadence

Observations:
>

---

## (c) Malformed event -> bsky.dlq.v1

Commands: `make inject-dlq` then `rpk topic consume bsky.dlq.v1 ... -n 1`.

- [ ] `dlq_inject_ok` logged by the injector
- [ ] One DLQ message; envelope carries `raw_payload`, `error` (KeyError on missing `text`),
      `intended_topic = app.bsky.feed.post`, `received_at`

Observations:
>

---

## Anything surprising / follow-ups

>

## Verdict

- [ ] v1 ingest verified end-to-end locally — ready to merge feat/v1-local-stack and move on
      to ClickHouse Kafka engine + MV.
