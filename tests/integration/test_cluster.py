"""Checks the running Docker stack against docker/kafka/topics.txt (`make test-integration`)."""

import urllib.request
from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient

from kafka_streaming.config import Settings

pytestmark = pytest.mark.integration

TOPICS_FILE = Path(__file__).parents[2] / "docker" / "kafka" / "topics.txt"


def expected_topics() -> dict[str, tuple[int, int]]:
    """Map topic name -> (partitions, replication factor) as declared in topics.txt."""
    specs = {}
    for line in TOPICS_FILE.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, partitions, rf, *_ = line.split()
        specs[name] = (int(partitions), int(rf))
    return specs


@pytest.fixture(scope="module")
def settings() -> Settings:
    return Settings.from_env()


def test_three_brokers_online(settings):
    metadata = AdminClient({"bootstrap.servers": settings.bootstrap_servers}).list_topics(
        timeout=10
    )
    assert len(metadata.brokers) == 3


def test_declared_topics_exist_with_layout(settings):
    metadata = AdminClient({"bootstrap.servers": settings.bootstrap_servers}).list_topics(
        timeout=10
    )

    for name, (partitions, rf) in expected_topics().items():
        assert name in metadata.topics, f"missing topic {name}"
        topic = metadata.topics[name]
        assert len(topic.partitions) == partitions
        assert all(len(p.replicas) == rf for p in topic.partitions.values())


def test_schema_registry_reachable(settings):
    with urllib.request.urlopen(f"{settings.schema_registry_url}/subjects", timeout=5) as resp:
        assert resp.status == 200
