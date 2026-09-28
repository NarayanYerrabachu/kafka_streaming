#!/usr/bin/env bash
# Creates every topic in topics.txt (idempotent). Runs inside the kafka-init container.
set -euo pipefail

KT=/opt/kafka/bin/kafka-topics.sh

grep -vE '^\s*(#|$)' /init/topics.txt | while read -r name partitions rf configs; do
  args=(--bootstrap-server "$BOOTSTRAP_SERVERS" --create --if-not-exists
        --topic "$name" --partitions "$partitions" --replication-factor "$rf")
  if [[ -n "${configs:-}" ]]; then
    IFS=',' read -ra kvs <<< "$configs"
    for kv in "${kvs[@]}"; do args+=(--config "$kv"); done
  fi
  "$KT" "${args[@]}"
  echo "ok: $name ($partitions partitions, rf=$rf)"
done

"$KT" --bootstrap-server "$BOOTSTRAP_SERVERS" --list
