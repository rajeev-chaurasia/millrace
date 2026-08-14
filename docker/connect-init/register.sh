#!/bin/sh
set -eu

: "${CONNECT_URL:?CONNECT_URL is required}"
: "${DEBEZIUM_CONNECTOR_NAME:?DEBEZIUM_CONNECTOR_NAME is required}"
: "${POSTGRES_DB:?POSTGRES_DB is required}"
: "${POSTGRES_USER:?POSTGRES_USER is required}"
: "${POSTGRES_PASSWORD:?POSTGRES_PASSWORD is required}"

config_file="$(mktemp)"
trap 'rm -f "$config_file"' EXIT

jq \
  --arg database "$POSTGRES_DB" \
  --arg user "$POSTGRES_USER" \
  --arg password "$POSTGRES_PASSWORD" \
  '.["database.dbname"] = $database
   | .["database.user"] = $user
   | .["database.password"] = $password' \
  /config/postgres-connector.json.template > "$config_file"
curl \
  --fail-with-body \
  --retry 12 \
  --retry-all-errors \
  --retry-delay 5 \
  --request PUT \
  --header "Content-Type: application/json" \
  --data-binary "@$config_file" \
  "${CONNECT_URL}/connectors/${DEBEZIUM_CONNECTOR_NAME}/config"
