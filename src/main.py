import asyncio
import json
import signal
import time
from collections import defaultdict
from types import FrameType
from typing import Any

from aiokafka import AIOKafkaProducer
from atproto import (
    CAR,
    AsyncFirehoseSubscribeReposClient,
    AtUri,
    firehose_models,
    models,
    parse_subscribe_repos_message,
)

_INTERESTED_RECORDS = {
    models.ids.AppBskyFeedLike: models.AppBskyFeedLike,
    models.ids.AppBskyFeedPost: models.AppBskyFeedPost,
    models.ids.AppBskyGraphFollow: models.AppBskyGraphFollow,
}

# k8s internal dns resolution for your strimzi cluster
KAFKA_BOOTSTRAP_SERVERS = 'homelab-broker-kafka-bootstrap.kafka-cluster.svc:9092'
KAFKA_TOPIC = 'bluesky-posts'

def _get_ops_by_type(commit: models.ComAtprotoSyncSubscribeRepos.Commit) -> defaultdict:
    operation_by_type = defaultdict(lambda: {'created': [], 'deleted': []})
    car = CAR.from_bytes(commit.blocks)
    for op in commit.ops:
        if op.action == 'update':
            continue

        uri = AtUri.from_str(f'at://{commit.repo}/{op.path}')

        if op.action == 'create':
            if not op.cid:
                continue

            create_info = {'uri': str(uri), 'cid': str(op.cid), 'author': commit.repo}
            record_raw_data = car.blocks.get(op.cid)
            if not record_raw_data:
                continue

            record = models.get_or_create(record_raw_data, strict=False)
            record_type = _INTERESTED_RECORDS.get(uri.collection)
            if record_type and models.is_record_type(record, record_type):
                operation_by_type[uri.collection]['created'].append({'record': record, **create_info})

        if op.action == 'delete':
            operation_by_type[uri.collection]['deleted'].append({'uri': str(uri)})

    return operation_by_type

def measure_events_per_second(func: callable) -> callable:
    def wrapper(*args) -> Any:
        wrapper.calls += 1
        cur_time = time.time()
        if cur_time - wrapper.start_time >= 1:
            print(f'NETWORK LOAD: {wrapper.calls} events/second')
            wrapper.start_time = cur_time
            wrapper.calls = 0
        return func(*args)

    wrapper.calls = 0
    wrapper.start_time = time.time()
    return wrapper

async def main() -> None:
    # initialize the kafka producer
    # producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
    # await producer.start()

    client = AsyncFirehoseSubscribeReposClient()

    async def signal_handler() -> None:
        print('shutting down gracefully...')
        await client.stop()
        # await producer.stop()

    # attach signal handlers to the current event loop
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, lambda: asyncio.create_task(signal_handler()))

    @measure_events_per_second
    async def on_message_handler(message: firehose_models.MessageFrame) -> None:
        commit = parse_subscribe_repos_message(message)
        if not isinstance(commit, models.ComAtprotoSyncSubscribeRepos.Commit):
            return

        if commit.seq % 20 == 0:
            client.update_params(models.ComAtprotoSyncSubscribeRepos.Params(cursor=commit.seq))

        if not commit.blocks:
            return

        ops = _get_ops_by_type(commit)
        for created_post in ops[models.ids.AppBskyFeedPost]['created']:
            author = created_post['author']
            record = created_post['record']
            inlined_text = record.text.replace('\n', ' ')
            
            payload = {
                "author": author,
                "created_at": record.created_at,
                "text": inlined_text
            }
            
            # serialize dictionary to bytes and send to kafka asynchronously
            try:
                print(f"[TEST SINK] {json.dumps(payload, indent=2)}")
                # await producer.send_and_wait(
                #     KAFKA_TOPIC, 
                #     value=json.dumps(payload).encode('utf-8')
                # )
                pass
            except Exception as e:
                print(f'kafka delivery failure: {e}')

    try:
        await client.start(on_message_handler)
    except asyncio.CancelledError:
        pass
    finally:
        # await producer.stop()
        pass

if __name__ == '__main__':
    asyncio.run(main())