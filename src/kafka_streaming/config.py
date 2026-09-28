"""Connection settings for the local streaming stack, overridable via environment variables."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    bootstrap_servers: str
    schema_registry_url: str

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            bootstrap_servers=os.getenv(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092,localhost:9094,localhost:9096"
            ),
            schema_registry_url=os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8085"),
        )
