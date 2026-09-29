"""Produces events through the real producer and reads them back with the real consumer."""

import uuid

import pytest

from kafka_streaming.config import Settings
from kafka_streaming.consumer import RecentChangeConsumer
from kafka_streaming.producer import DLQ_TOPIC, RAW_TOPIC, WikimediaProducer
from tests.test_events import RAW

pytestmark = pytest.mark.integration


def test_produce_then_consume():
    settings = Settings.from_env()
    run_id = str(uuid.uuid4())
    # A fresh group starting at "latest" only sees what we produce after subscribing.
    consumer = RecentChangeConsumer(settings, group_id=f"test-{run_id}", topic=RAW_TOPIC)
    stream = consumer.messages(poll_timeout=0.5)
    # First poll triggers partition assignment; wait for it before producing.
    consumer._consumer.poll(5)

    producer = WikimediaProducer(settings)
    events = [{**RAW, "meta": {**RAW["meta"], "id": f"{run_id}-{i}"}} for i in range(5)]
    for raw in events:
        producer.send(raw)
    assert producer.flush() == 0
    assert producer.sent == 5 and producer.failed == 0

    received = {}
    for _msg, record in stream:
        if record["event_id"].startswith(run_id):
            received[record["event_id"]] = record
        if len(received) == 5:
            break
    consumer.close()

    assert received.keys() == {e["meta"]["id"] for e in events}
    assert all(r["server_name"] == "en.wikipedia.org" for r in received.values())


def test_bad_event_goes_to_dlq():
    settings = Settings.from_env()
    producer = WikimediaProducer(settings)

    producer.send({"garbage": True})
    assert producer.flush() == 0

    assert producer.dlq == 1
    assert producer.sent == 1  # the DLQ write itself was delivered
    assert producer.dlq_topic == DLQ_TOPIC
