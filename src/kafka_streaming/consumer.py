"""Reads Avro records back from Kafka and prints them.

ks-consume --max-messages 20
ks-consume --from-beginning --group my-replay
"""

import argparse
import logging
import signal
from collections.abc import Iterator
from typing import Any

from confluent_kafka import Consumer, KafkaError, Message
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import MessageField, SerializationContext

from kafka_streaming.config import Settings
from kafka_streaming.producer import RAW_TOPIC

log = logging.getLogger(__name__)


class RecentChangeConsumer:
    def __init__(
        self,
        settings: Settings,
        group_id: str,
        topic: str = RAW_TOPIC,
        from_beginning: bool = False,
    ):
        self.topic = topic
        registry = SchemaRegistryClient({"url": settings.schema_registry_url})
        self._value = AvroDeserializer(registry)  # schema is fetched by id from each message
        self._consumer = Consumer(
            {
                "bootstrap.servers": settings.bootstrap_servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest" if from_beginning else "latest",
                "enable.auto.commit": False,
            }
        )
        self._consumer.subscribe([topic])

    def messages(self, poll_timeout: float = 1.0) -> Iterator[tuple[Message, dict[str, Any]]]:
        """Yield (message, decoded record) forever; commit after each record."""
        ctx = SerializationContext(self.topic, MessageField.VALUE)
        while True:
            msg = self._consumer.poll(poll_timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(msg.error())
            record = self._value(msg.value(), ctx)
            yield msg, record
            self._consumer.commit(msg, asynchronous=False)

    def close(self) -> None:
        self._consumer.close()


def format_record(msg: Message, record: dict[str, Any]) -> str:
    who = "bot" if record["bot"] else "human"
    return (
        f"[p{msg.partition()}@{msg.offset()}] {record['event_time']:%H:%M:%S} "
        f"{record['server_name']:<22} {record['type']:<10} {who:<5} "
        f"{record['user'][:20]:<20} {record['title'][:60]}"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Read Wikimedia edits from Kafka")
    parser.add_argument("--group", default="ks-console", help="consumer group id")
    parser.add_argument("--topic", default=RAW_TOPIC)
    parser.add_argument("--from-beginning", action="store_true")
    parser.add_argument("--max-messages", type=int, default=None)
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    consumer = RecentChangeConsumer(
        Settings.from_env(), args.group, topic=args.topic, from_beginning=args.from_beginning
    )
    stop = False

    def _stop(*_):
        nonlocal stop
        stop = True

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    count = 0
    try:
        for msg, record in consumer.messages():
            print(format_record(msg, record), flush=True)
            count += 1
            if stop or (args.max_messages and count >= args.max_messages):
                break
    finally:
        consumer.close()
        log.info("consumed %d messages", count)


if __name__ == "__main__":
    main()
