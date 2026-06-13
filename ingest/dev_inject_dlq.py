"""Dev/verification helper — inject ONE malformed event into ``bsky.dlq.v1``.

NOT a production entrypoint. Real Jetstream data is well-formed, so the DLQ never fires
on its own; this drives a single known-bad event through the *same* validate->produce_dlq
path the ingest loop uses (see ``ingest/main.py``) so you can watch it land in the DLQ.

It deliberately reuses the real collaborators — ``to_model`` (to raise the genuine parse
exception) and ``IngestProducer.produce_dlq`` (the real Avro-serialized DLQ produce against
the running Schema Registry) — rather than hand-building a ``DlqEnvelope``, so what you
observe is exactly what the service would do.

Run inside the compose network so it reuses the ingest service env::

    docker compose --profile ingest run --rm --no-deps ingest python -m ingest.dev_inject_dlq

The mounted fixture (``tests/fixtures/malformed_post_event.json``) is a post-create record
missing the required ``text`` field, so ``to_model`` raises ``KeyError`` and the event is
routed to the DLQ.
"""

from __future__ import annotations

import json
import pathlib
import sys
from typing import Any

import structlog
from pydantic import ValidationError

from ingest.config import Settings
from ingest.parse import to_model
from ingest.producer import IngestProducer, serialize_raw

log = structlog.get_logger("ingest.dev_inject_dlq")

_FIXTURE = pathlib.Path("tests/fixtures/malformed_post_event.json")


def _collection(event: dict[str, Any]) -> str:
    commit = event.get("commit")
    return commit.get("collection", "unknown") if isinstance(commit, dict) else "unknown"


def main() -> None:
    settings = Settings()
    event: dict[str, Any] = json.loads(_FIXTURE.read_text())
    collection = _collection(event)

    # Mirror the ingest loop's parse boundary: the malformed fixture must raise here.
    try:
        to_model(event)
    except (ValidationError, KeyError, TypeError) as exc:
        producer = IngestProducer(settings)
        producer.produce_dlq(serialize_raw(event), repr(exc), intended_topic=str(collection))
        remaining = producer.flush(10.0)
        if remaining:
            log.error("dlq_inject_unflushed", remaining=remaining)
            sys.exit(1)
        log.info("dlq_inject_ok", intended_topic=collection, error=repr(exc))
        return

    # The fixture is supposed to be malformed; if it parsed, the test premise is broken.
    log.error("dlq_inject_unexpectedly_valid", fixture=str(_FIXTURE))
    sys.exit(1)


if __name__ == "__main__":
    main()
