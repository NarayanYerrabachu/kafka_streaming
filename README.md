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
make test-integration # producer/consumer tests on a throwaway Kafka (Testcontainers)
make test-stack       # checks the running docker-compose stack
make produce          # stream live Wikipedia edits into Kafka (Ctrl-C to stop)
make consume          # print them back as they arrive (Ctrl-C to stop)
make help             # all targets
```

## Producer and consumer

`ks-produce` reads the [Wikimedia recentchange](https://stream.wikimedia.org/v2/stream/recentchange) live feed (about 20–50 events/s), maps each event to the Avro schema in `schemas/recentchange.avsc`, and writes it to `wikimedia.recentchange.raw`. The Kafka key is the wiki host (for example `en.wikipedia.org`), so one wiki's events stay in order on one partition. Events that fail parsing go to `wikimedia.recentchange.dlq` as raw JSON with the error. The producer is idempotent and waits for all in-sync replicas (`acks=all`).

`ks-consume` reads the topic with a consumer group, decodes Avro through Schema Registry, and prints one line per event. Offsets are committed after each record. Useful flags:

```bash
.venv/bin/ks-produce --max-events 500
.venv/bin/ks-consume --max-messages 20
.venv/bin/ks-consume --from-beginning --group replay-1   # new group: read everything
```

Both stop cleanly on Ctrl-C. Run them in two terminals to watch events flow.

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

## Tests

| Command                 | What it does                                                                                        | Needs |
|-------------------------|-----------------------------------------------------------------------------------------------------|-------|
| `make test`             | Unit tests: event mapping, schema validation, settings                                             | nothing |
| `make test-integration` | Starts a single-broker Kafka (KRaft) and Schema Registry with [Testcontainers](https://testcontainers-python.readthedocs.io/), creates the topics from `topics.txt`, then runs the real producer and consumer against them: round-trip, partitioning by wiki, DLQ routing. Containers are removed afterwards. About 30 s. | Docker |
| `make test-stack`       | Checks the running compose stack: 3 brokers, topic layout, Schema Registry reachable               | `make up` |

The integration tests are self-contained, so they can run in CI without the compose stack.

## Configuration

| Env var                   | Default                                        |
|---------------------------|------------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092,localhost:9094,localhost:9096` |
| `SCHEMA_REGISTRY_URL`     | `http://localhost:8085`                        |

## Roadmap

- [x] **Phase 1: Foundation.** Kafka cluster, topics, Schema Registry, UI, tooling
- [x] **Phase 2a: Ingestion.** Avro schema, Wikimedia producer, DLQ, console consumer
- [ ] **Phase 2b:** Synthetic clickstream generator for load tests
- [ ] **Phase 3: Stream processing.** Spark to Iceberg on MinIO, windowed aggregates, watermarks, exactly-once writes
- [ ] **Phase 4: Serving.** ClickHouse sink, Grafana dashboards
- [ ] **Phase 5: Reliability and scale.** Lag monitoring (Prometheus), load tests, offset replay
