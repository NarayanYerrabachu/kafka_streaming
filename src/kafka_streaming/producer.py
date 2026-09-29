"""Streams live Wikimedia edits into Kafka as Avro records.

    ks-produce --max-events 100

Events that fail parsing go to the DLQ topic as raw JSON, so nothing is silently dropped.
"""

import argparse
import json
import logging
import signal
from collections.abc import Iterator
from typing import Any

import requests
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

from kafka_streaming.config import Settings
from kafka_streaming.events import load_schema, to_record

log = logging.getLogger(__name__)

WIKIMEDIA_STREAM = "https://stream.wikimedia.org/v2/stream/recentchange"
USER_AGENT = "kafka-streaming-dev/0.1 (https://github.com/NarayanYerrabachu/kafka_streaming)"
RAW_TOPIC = "wikimedia.recentchange.raw"
DLQ_TOPIC = "wikimedia.recentchange.dlq"


def sse_events(url: str = WIKIMEDIA_STREAM, timeout: float = 60) -> Iterator[dict[str, Any]]:
    """Yield the JSON payload of each server-sent event; reconnects if the stream drops."""
    while True:
        try:
            with requests.get(
                url, stream=True, headers={"User-Agent": USER_AGENT}, timeout=timeout
            ) as resp:
                resp.raise_for_status()
                for line in resp.iter_lines(decode_unicode=True):
                    if line and line.startswith("data:"):
                        yield json.loads(line[5:])
        except (requests.RequestException, json.JSONDecodeError) as exc:
            log.warning("stream interrupted (%s), reconnecting", exc)


class WikimediaProducer:
    def __init__(self, settings: Settings, raw_topic: str = RAW_TOPIC, dlq_topic: str = DLQ_TOPIC):
        self.raw_topic = raw_topic
        self.dlq_topic = dlq_topic
        self.sent = self.failed = self.dlq = 0

        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._value = AvroSerializer(registry, load_schema())
        self._key = StringSerializer("utf_8")
        self._producer = Producer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "compression.type": "lz4",
                "linger.ms": 50,
            }
        )

    def send(self, raw: dict[str, Any]) -> None:
        """Serialize and enqueue one raw event; routes unparseable events to the DLQ."""
        ctx = SerializationContext(self.raw_topic, MessageField.VALUE)
        try:
            record = to_record(raw)
            self._producer.produce(
                self.raw_topic,
                key=self._key(record["server_name"]),
                value=self._value(record, ctx),
                on_delivery=self._on_delivery,
            )
        except (ValueError, TypeError, KeyError) as exc:
            self.dlq += 1
            self._producer.produce(
                self.dlq_topic,
                value=json.dumps({"error": str(exc), "raw": raw}).encode(),
                on_delivery=self._on_delivery,
            )
        self._producer.poll(0)

    def flush(self, timeout: float = 30) -> int:
        return self._producer.flush(timeout)

    def _on_delivery(self, err, msg) -> None:
        if err is not None:
            self.failed += 1
            log.error("delivery failed: %s", err)
        else:
            self.sent += 1


def main() -> None:
    parser = argparse.ArgumentParser(description="Stream Wikimedia edits into Kafka")
    parser.add_argument("--max-events", type=int, default=None, help="stop after N events")
    parser.add_argument("--topic", default=RAW_TOPIC)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    producer = WikimediaProducer(Settings.from_env(), raw_topic=args.topic)
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    for n, raw in enumerate(sse_events(), start=1):
        producer.send(raw)
        if n % 100 == 0:
            p = producer
            log.info("read=%d sent=%d dlq=%d failed=%d", n, p.sent, p.dlq, p.failed)
        if stop or (args.max_events and n >= args.max_events):
            break

    remaining = producer.flush()
    log.info(
        "done: sent=%d dlq=%d failed=%d unflushed=%d",
        producer.sent,
        producer.dlq,
        producer.failed,
        remaining,
    )


if __name__ == "__main__":
    main()
