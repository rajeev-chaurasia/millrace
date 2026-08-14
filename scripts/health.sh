#!/bin/sh
set -eu

dashboard=false
monitoring=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --dashboard)
      dashboard=true
      ;;
    --monitoring)
      monitoring=true
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file="$project_root/compose.yaml"
env_file="$project_root/.env"

if [ ! -f "$env_file" ]; then
  echo ".env is missing. Run scripts/bootstrap.sh first." >&2
  exit 1
fi

compose() {
  docker compose --env-file "$env_file" --file "$compose_file" "$@"
}

services="postgres kafka connect spark-master spark-worker minio airflow-api-server airflow-scheduler airflow-dag-processor"
if [ "$dashboard" = true ]; then
  services="$services dashboard"
fi
if [ "$monitoring" = true ]; then
  services="$services prometheus pushgateway grafana"
fi

failures=0
for service in $services; do
  container_id=$(compose ps --quiet "$service")
  if [ -z "$container_id" ]; then
    printf '%-24s %s\n' "$service" "no container"
    failures=$((failures + 1))
    continue
  fi

  state=$(docker inspect --format '{{.State.Status}}' "$container_id")
  health=$(docker inspect \
    --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}not-configured{{end}}' \
    "$container_id")
  printf '%-24s state=%-10s health=%s\n' "$service" "$state" "$health"

  if [ "$state" != "running" ]; then
    failures=$((failures + 1))
  fi
  case "$health" in
    healthy|not-configured) ;;
    *) failures=$((failures + 1)) ;;
  esac
done

connector_name=$(awk -F= '$1 == "DEBEZIUM_CONNECTOR_NAME" { print substr($0, index($0, "=") + 1); exit }' "$env_file")
if [ -z "$connector_name" ]; then
  echo "DEBEZIUM_CONNECTOR_NAME is missing from .env." >&2
  exit 1
fi

if status_json=$(compose exec -T connect \
  curl --fail --silent "http://localhost:8083/connectors/$connector_name/status"); then
  case "$status_json" in
    *'"state":"FAILED"'*|*'"state": "FAILED"'*)
      echo "$connector_name connector has a failed component."
      failures=$((failures + 1))
      ;;
    *'"connector":{"state":"RUNNING"'*|*'"connector": {"state": "RUNNING"'*)
      echo "$connector_name connector is running."
      ;;
    *)
      echo "$connector_name connector is not running."
      failures=$((failures + 1))
      ;;
  esac
else
  echo "$connector_name connector status is unavailable."
  failures=$((failures + 1))
fi

if [ "$failures" -ne 0 ]; then
  echo "Health check failed with $failures unhealthy condition(s)." >&2
  exit 1
fi

echo "Millrace services are healthy."
