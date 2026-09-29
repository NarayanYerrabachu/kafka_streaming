COMPOSE := docker compose -f docker/docker-compose.yml -p kafka-streaming
PYTHON  ?= python3

.PHONY: help install up down clean ps logs topics produce consume test test-integration lint

help:  ## Show available targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-18s %s\n", $$1, $$2}'

install:  ## Create .venv and install package + dev tools
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -e '.[dev]'
	.venv/bin/pre-commit install

up:  ## Start Kafka cluster, create topics, start Schema Registry + UI
	$(COMPOSE) up -d --wait

down:  ## Stop the stack (keeps data)
	$(COMPOSE) down

clean:  ## Stop the stack and delete all Kafka data
	$(COMPOSE) down -v

ps:  ## Show container status
	$(COMPOSE) ps

logs:  ## Follow logs of all services
	$(COMPOSE) logs -f

topics:  ## Describe all topics
	docker exec ks-kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --describe

produce:  ## Stream live Wikipedia edits into Kafka (Ctrl-C to stop)
	.venv/bin/ks-produce

consume:  ## Print events from Kafka as they arrive (Ctrl-C to stop)
	.venv/bin/ks-consume

test:  ## Unit tests (no Docker needed)
	.venv/bin/pytest

test-integration:  ## Tests against the running stack (run `make up` first)
	.venv/bin/pytest -m integration

lint:  ## Run all pre-commit hooks
	.venv/bin/pre-commit run --all-files
