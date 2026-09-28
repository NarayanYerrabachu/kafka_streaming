from kafka_streaming.config import Settings


def test_defaults_point_at_local_stack(monkeypatch):
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("SCHEMA_REGISTRY_URL", raising=False)

    settings = Settings.from_env()

    assert settings.bootstrap_servers == "localhost:9092,localhost:9094,localhost:9096"
    assert settings.schema_registry_url == "http://localhost:8085"


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker:9092")
    monkeypatch.setenv("SCHEMA_REGISTRY_URL", "http://sr:8081")

    settings = Settings.from_env()

    assert settings.bootstrap_servers == "broker:9092"
    assert settings.schema_registry_url == "http://sr:8081"
