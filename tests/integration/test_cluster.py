"""Checks the docker-compose stack against docker/kafka/topics.txt (`make test-stack`)."""

import urllib.request

import pytest
from confluent_kafka.admin import AdminClient

from kafka_streaming.config import Settings
from tests.integration.conftest import declared_topics

pytestmark = pytest.mark.stack


@pytest.fixture(scope="module")
def stack_settings() -> Settings:
    return Settings.from_env()


def test_three_brokers_online(stack_settings):
    metadata = AdminClient({"bootstrap.servers": stack_settings.bootstrap_servers}).list_topics(
        timeout=10
    )
    assert len(metadata.brokers) == 3


def test_declared_topics_exist_with_layout(stack_settings):
    metadata = AdminClient({"bootstrap.servers": stack_settings.bootstrap_servers}).list_topics(
        timeout=10
    )

    for name, (partitions, rf) in declared_topics().items():
        assert name in metadata.topics, f"missing topic {name}"
        topic = metadata.topics[name]
        assert len(topic.partitions) == partitions
        assert all(len(p.replicas) == rf for p in topic.partitions.values())


def test_schema_registry_reachable(stack_settings):
    url = f"{stack_settings.schema_registry_url}/subjects"
    with urllib.request.urlopen(url, timeout=5) as resp:
        assert resp.status == 200
