"""Throwaway Kafka + Schema Registry for integration tests, started once per test session.

Needs Docker, nothing else: the docker-compose stack does not have to be running.
Topics come from docker/kafka/topics.txt, created with replication factor 1 (single broker).
"""

from pathlib import Path

import pytest
from confluent_kafka.admin import AdminClient, NewTopic
from testcontainers.community.kafka import KafkaContainer
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

from kafka_streaming.config import Settings

KAFKA_IMAGE = "confluentinc/cp-kafka:7.7.1"
SCHEMA_REGISTRY_IMAGE = "confluentinc/cp-schema-registry:7.7.1"
TOPICS_FILE = Path(__file__).parents[2] / "docker" / "kafka" / "topics.txt"


def declared_topics() -> dict[str, tuple[int, int]]:
    """Map topic name -> (partitions, replication factor) as declared in topics.txt."""
    specs = {}
    for line in TOPICS_FILE.read_text().splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        name, partitions, rf, *_ = line.split()
        specs[name] = (int(partitions), int(rf))
    return specs


@pytest.fixture(scope="session")
def kafka_stack():
    """Yield (bootstrap_servers, schema_registry_url) for a fresh single-broker cluster."""
    with Network() as network:
        kafka = (
            KafkaContainer(KAFKA_IMAGE)
            .with_kraft()
            .with_network(network)
            .with_network_aliases("kafka")
        )
        with kafka:
            registry = (
                DockerContainer(SCHEMA_REGISTRY_IMAGE)
                .with_network(network)
                .with_env("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
                .with_env("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
                .with_env("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                .with_exposed_ports(8081)
                .waiting_for(LogMessageWaitStrategy("Server started").with_startup_timeout(60))
            )
            with registry:
                bootstrap = kafka.get_bootstrap_server()
                _create_topics(bootstrap)
                url = f"http://{registry.get_container_host_ip()}:{registry.get_exposed_port(8081)}"
                yield bootstrap, url


def _create_topics(bootstrap: str) -> None:
    admin = AdminClient({"bootstrap.servers": bootstrap})
    new = [
        NewTopic(name, num_partitions=partitions, replication_factor=1)
        for name, (partitions, _rf) in declared_topics().items()
    ]
    for future in admin.create_topics(new).values():
        future.result(timeout=30)


@pytest.fixture(scope="session")
def settings(kafka_stack) -> Settings:
    bootstrap, registry_url = kafka_stack
    return Settings(bootstrap_servers=bootstrap, schema_registry_url=registry_url)
