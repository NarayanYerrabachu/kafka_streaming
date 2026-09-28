# kafka_streaming

A local big-data streaming pipeline built on Kafka and Spark Structured Streaming, with Iceberg and ClickHouse. Everything runs in Docker on one machine.

```
 Wikimedia live edits ─┐                         ┌─▶ MinIO + Iceberg (data lake)
                       ├─▶ Kafka (3 brokers) ──▶ Spark Structured Streaming
 Synthetic generator ──┘   + Schema Registry     └─▶ ClickHouse ─▶ Grafana
```

## Quick start

```bash
make install          # .venv + dev tools + pre-commit hook
make up               # 3-node Kafka (KRaft), topics, Schema Registry, Kafka UI
make test             # unit tests
make test-integration # checks the running stack
make help             # all targets
```

| Service         | Host address                                   |
|-----------------|------------------------------------------------|
| Kafka brokers   | `localhost:9092`, `localhost:9094`, `localhost:9096` |
| Schema Registry | http://localhost:8085                           |
| Kafka UI        | http://localhost:8088                           |

Inside Docker, the containers reach the brokers at `kafka-{1,2,3}:19092`.

## Topics

`docker/kafka/topics.txt` is the only place topics are defined. The `kafka-init` container creates them on `make up`, and the integration tests check the running cluster against the same file. To add a topic, add a line to that file and run `make up` again.

| Topic                        | Partitions | Purpose                        |
|------------------------------|-----------:|--------------------------------|
| `wikimedia.recentchange.raw` | 6          | Live Wikipedia edit events     |
| `wikimedia.recentchange.dlq` | 3          | Events that failed validation  |
| `clickstream.events`         | 12         | Synthetic load-test events     |

All topics use replication factor 3 with `min.insync.replicas=2`, so the cluster keeps accepting writes if one broker goes down.

## Configuration

| Env var                   | Default                                        |
|---------------------------|------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` |
| `SCHEMA_REGISTRY_URL`     | `http://localhost:8085`                        |

## Roadmap

- [x] **Phase 1: Foundation.** Kafka cluster, topics, Schema Registry, UI, tooling
- [ ] **Phase 2: Ingestion.** Avro schemas, Wikimedia producer, DLQ, synthetic generator
- [ ] **Phase 3: Stream processing.** Spark to Iceberg on MinIO, windowed aggregates, watermarks, exactly-once writes
- [ ] **Phase 4: Serving.** ClickHouse sink, Grafana dashboards
- [ ] **Phase 5: Reliability and scale.** Lag monitoring (Prometheus), load tests, offset replay
