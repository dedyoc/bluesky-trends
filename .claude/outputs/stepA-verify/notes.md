# Step A — verify notes (v1 ingest, local end-to-end)

_Stub for you to fill in as you run the VERIFY.md runbook by hand._
_Date run: ____  ·  Stack: docker-compose.dev.yml on branch feat/v1-local-stack_

Runbook: `../../../VERIFY.md`. Infra was left running from this session and the slate was
reset (empty cursor, fresh topics). `make up` is idempotent if you need to restart it.

---

## (a) Events flowing into bsky.posts.v1

Commands run: `make run-ingest` (terminal 1) + `rpk topic consume bsky.posts.v1 ... -n 5`.

- [x] `jetstream_connected` appeared in the logs
- [x] `ingest_stats` shows `events_total` climbing, `last_event_age_s` ≈ 0, `dlq_total: 0`
- [x] 5 Avro-framed messages consumed from bsky.posts.v1

Observations:
> As this is the first time it ran, no resume_cursor was set, when consuming, we can see that the value is Avro-encoded format:
{
  "topic": "bsky.posts.v1",
  "key": "did:plc:4jvzjx6sllvctlw3k7i4ckgb",
  "value": "\u0000\u0000\u0000\u0000\u0003@did:plc:4jvzjx6sllvctlw3k7i4ckgb\u001a3mo7fhe47xk23vbafyreieheoipsk5xcy3trxynoo4s4pufhqsqrao3amypuia7m3qvy5ibra\ufffdވ\ufffdۊ\ufffd\u0006\u0012Monke \u003e:3\u0002\u0002\u0004en\u0000\u0002\ufffd\u0001at://did:plc:l5kleijjctgq5wm3lj3cnm7s/app.bsky.feed.post/3mo7ffgqxqk2h\u0002\ufffd\u0001at://did:plc:l5kleijjctgq5wm3lj3cnm7s/app.bsky.feed.post/3mo7ffgqxqk2h",
  "timestamp": 1781392895282,
  "partition": 0,
  "offset": 0
}

---

## (b1) Graceful stop — NO duplicates

- Cursor before Ctrl-C (C1): `1781393224590962`
- Cursor after Ctrl-C (C2, should be > C1): `1781393293822928`
- [x] On restart, logs show `resume_cursor=1781393293822928` / `jetstream_connected cursor=1781393293822928`
- [x] No duplicate overlap observed

Observations:
> Graceful Ctrl-C printed `ingest_draining` then a final `ingest_stats` before exit — i.e.
> the `finally` block ran `producer.flush()` + `checkpointer.final_save()`. C2 (after) was
> strictly greater than C1 (before): the cursor advanced to the latest acked event. On
> restart, `resume_cursor` == C2 exactly, so nothing already-produced was replayed → no
> duplicates. This is the expected graceful path.

## (b2) Hard crash (SIGKILL) — duplicate overlap

Kill: `docker compose -f docker-compose.dev.yml kill -s SIGKILL ingest` (from a 2nd terminal).

- Cursor after SIGKILL (C3, last periodic checkpoint): `1781410279635436`
- [x] On restart, logs show `resume_cursor=1781410279635436` (== C3)
- [x] Replay overlap observed (events between C3 and the kill re-produced), bounded by the
      100-ack / 2s checkpoint cadence

Observations:
> Confirmed end-to-end. After SIGKILL there were **zero** `ingest_draining` lines in the log
> (`docker compose logs ingest | grep -c ingest_draining` == 0) — proving `final_save` never
> ran. C3 was the last *periodic* checkpoint (100-ack / 2s timer), which sat BEHIND the last
> produced event: posts high-watermark was 9485 at the kill, but C3 only covered up to the
> previous 2s checkpoint. On restart `resume_cursor` == C3, so Jetstream replayed every event
> with `time_us > C3` that had already been produced; the posts watermark grew 9485 → 10795
> across the restart, and the replayed tail reappears at NEW Kafka offsets.
>
> **How to actually SEE a duplicate** (the part I was unsure about): the cursor is the proof.
> The single observable fact is that on restart `resume_cursor` is LOWER than the highest
> event already produced before the kill — so everything in between is re-sent. To eyeball a
> repeated record, consume a bounded slice straddling the restart boundary and look for the
> same `(did, rkey)` at two different offsets, e.g.:
>
> ```bash
> # offsets just before the kill watermark .. a bit after restart; rkey is the at://.../<rkey>
> docker compose -f docker-compose.dev.yml exec redpanda \
>   rpk topic consume bsky.posts.v1 -X brokers=redpanda:9092 -o <wm_before-50> -n 120 -f '%o %k\n'
> ```
>
> The same DID key (and, in the Avro value, the same rkey) appearing at two offsets is the
> overlap. Note rpk binary-value formatting over the host pipe is finicky (`-o start:end`
> and `-e` misbehaved in v24.2.x); bounded `-n <count>` with `-o <offset>` is the reliable
> form. The math above (resume_cursor < produced-watermark) is the authoritative signal and
> needs no value parsing.
>
> Downstream sinks dedupe this overlap on `(did, rkey/cid)` — so at-least-once here is by
> design, not a defect.

---

## (c) Malformed event -> bsky.dlq.v1

Commands: `make inject-dlq` then `rpk topic consume bsky.dlq.v1 ... -n 1`.

- [x] `dlq_inject_ok` logged by the injector
- [x] One DLQ message; envelope carries `raw_payload`, `error` (KeyError on missing `text`),
      `intended_topic = app.bsky.feed.post`, `received_at`

Observations:
> Worked first try. One gotcha worth remembering (now also noted in VERIFY.md): the running
> ingest service's `dlq_total` stays **0** after `make inject-dlq`. That's correct — the
> injector runs as a *separate* `docker compose run` container, and `dlq_total` is an
> in-memory per-process counter. Verify via the topic, not the service log. A non-zero
> service `dlq_total` would mean the live stream itself produced a malformed record.

---

## Anything surprising / follow-ups

> Two real bugs surfaced during verification (both fixed + committed on feat/v1-local-stack;
> logged in defects.md):
> 1. **Producer crash** — `_to_avro_dict` took `(model)` but the Confluent `AvroSerializer`
>    calls `to_dict(obj, ctx)`; every produce raised TypeError. The avro round-trip unit
>    test missed it (it drives fastavro directly, not the real serializer). Caught only by
>    the end-to-end run (posts high-watermark stuck at 0).
> 2. **Redpanda dual listeners** — the broker advertised only `localhost:9092`, so in-network
>    clients (ingest, topic-init) connected to `redpanda:9092` and got redirected to
>    localhost → refused. Fixed with internal (`redpanda:9092`) + external (`localhost:19092`)
>    listeners; host Kafka port is now **19092**.
>
> Plus a dev-ergonomics fix: the Dockerfile `CMD` and the `inject-dlq` target used `uv run`
> without `--no-dev`, so the dev group (mypy/ruff) re-installed on every container start —
> slow/noisy, and it broke a fast SIGKILL test (the kill landed mid-sync before ingest
> connected). Both now use `uv run --no-dev`.

## Verdict

- [x] v1 ingest verified end-to-end locally — events flow (a), graceful resume = no dups (b1),
      SIGKILL resume = bounded replay overlap (b2), malformed → DLQ (c). Ready to merge
      feat/v1-local-stack and move on to ClickHouse Kafka engine + MV.
