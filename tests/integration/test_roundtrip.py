"""Produces events through the real producer and reads them back with the real consumer."""

import json
import uuid

import pytest
from confluent_kafka import Consumer

from kafka_streaming.consumer import RecentChangeConsumer
from kafka_streaming.producer import DLQ_TOPIC, RAW_TOPIC, WikimediaProducer
from tests.test_events import RAW

pytestmark = pytest.mark.integration


def test_produce_then_consume(settings):
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


def test_same_wiki_lands_on_one_partition(settings):
    """Keying by server_name keeps one wiki's events ordered on a single partition."""
    run_id = str(uuid.uuid4())
    consumer = RecentChangeConsumer(settings, group_id=f"test-{run_id}", topic=RAW_TOPIC)
    stream = consumer.messages(poll_timeout=0.5)
    consumer._consumer.poll(5)

    producer = WikimediaProducer(settings)
    for i in range(10):
        producer.send({**RAW, "meta": {**RAW["meta"], "id": f"{run_id}-{i}"}})
    assert producer.flush() == 0

    partitions = set()
    seen = 0
    for msg, record in stream:
        if record["event_id"].startswith(run_id):
            partitions.add(msg.partition())
            seen += 1
        if seen == 10:
            break
    consumer.close()

    assert len(partitions) == 1


def test_bad_event_goes_to_dlq(settings):
    marker = str(uuid.uuid4())
    dlq = Consumer(
        {
            "bootstrap.servers": settings.bootstrap_servers,
            "group.id": f"dlq-{marker}",
            "auto.offset.reset": "earliest",
        }
    )
    dlq.subscribe([DLQ_TOPIC])
    dlq.poll(5)

    producer = WikimediaProducer(settings)
    producer.send({"garbage": True, "marker": marker})
    assert producer.flush() == 0
    assert producer.dlq == 1
    assert producer.sent == 1  # the DLQ write itself was delivered

    payload = None
    for _ in range(40):
        msg = dlq.poll(0.5)
        if msg is None or msg.error():
            continue
        candidate = json.loads(msg.value())
        if candidate["raw"].get("marker") == marker:
            payload = candidate
            break
    dlq.close()

    assert payload is not None, "DLQ record not found"
    assert "missing fields" in payload["error"]
    assert payload["raw"] == {"garbage": True, "marker": marker}
