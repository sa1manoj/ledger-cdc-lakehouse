#!/usr/bin/env bash
# Registers (or updates) the Debezium Postgres connector with Kafka Connect.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CONFIG_FILE="$(dirname "$0")/../connectors/postgres-ledger.json"
NAME="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["name"])' "$CONFIG_FILE")"

echo "Waiting for Kafka Connect at ${CONNECT_URL} ..."
until curl -sf "${CONNECT_URL}/connectors" > /dev/null; do sleep 3; done

# PUT /connectors/<name>/config is idempotent: creates or updates.
python3 -c 'import json,sys; print(json.dumps(json.load(open(sys.argv[1]))["config"]))' "$CONFIG_FILE" \
  | curl -sf -X PUT -H "Content-Type: application/json" --data @- \
      "${CONNECT_URL}/connectors/${NAME}/config"
echo
curl -s "${CONNECT_URL}/connectors/${NAME}/status"
echo
