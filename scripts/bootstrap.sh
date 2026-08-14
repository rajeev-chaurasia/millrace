#!/bin/sh
set -eu

dashboard=false
monitoring=false
timeout_seconds=1200

while [ "$#" -gt 0 ]; do
  case "$1" in
    --dashboard)
      dashboard=true
      ;;
    --monitoring)
      monitoring=true
      ;;
    --timeout)
      shift
      [ "$#" -gt 0 ] || {
        echo "--timeout requires a value" >&2
        exit 2
      }
      timeout_seconds=$1
      ;;
    *)
      echo "Unknown argument: $1" >&2
      exit 2
      ;;
  esac
  shift
done

case "$timeout_seconds" in
  *[!0-9]*|'')
    echo "--timeout must be an integer" >&2
    exit 2
    ;;
esac

script_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
project_root=$(CDPATH= cd -- "$script_dir/.." && pwd)
compose_file="$project_root/compose.yaml"
env_file="$project_root/.env"
env_example="$project_root/.env.example"

command -v docker >/dev/null 2>&1 || {
  echo "Docker is not available on PATH." >&2
  exit 1
}
docker compose version >/dev/null

if [ ! -f "$env_file" ]; then
  cp "$env_example" "$env_file"
  echo "Created .env from local-only example values."
else
  added_defaults=false
  while IFS= read -r line || [ -n "$line" ]; do
    case "$line" in
      [A-Za-z_]*=*)
        key=${line%%=*}
        if ! awk -F= -v expected="$key" '$1 == expected { found=1 } END { exit !found }' "$env_file"; then
          printf '%s\n' "$line" >> "$env_file"
          added_defaults=true
        fi
        ;;
    esac
  done < "$env_example"
  if [ "$added_defaults" = true ]; then
    echo "Added missing local-only defaults to .env."
  fi
fi

compose() {
  docker compose --env-file "$env_file" --file "$compose_file" "$@"
}

if [ "$dashboard" = true ] && [ "$monitoring" = true ]; then
  compose --profile dashboard --profile monitoring up --detach --build
elif [ "$dashboard" = true ]; then
  compose --profile dashboard up --detach --build
elif [ "$monitoring" = true ]; then
  compose --profile monitoring up --detach --build
else
  compose up --detach --build
fi

wait_services="postgres kafka connect spark-master spark-worker minio airflow-api-server airflow-scheduler airflow-dag-processor"
if [ "$dashboard" = true ]; then
  wait_services="$wait_services dashboard"
fi
if [ "$monitoring" = true ]; then
  wait_services="$wait_services prometheus pushgateway grafana"
fi

if [ "$dashboard" = true ] && [ "$monitoring" = true ]; then
  compose --profile dashboard --profile monitoring up --detach --no-deps \
    --wait --wait-timeout "$timeout_seconds" $wait_services
elif [ "$dashboard" = true ]; then
  compose --profile dashboard up --detach --no-deps \
    --wait --wait-timeout "$timeout_seconds" $wait_services
elif [ "$monitoring" = true ]; then
  compose --profile monitoring up --detach --no-deps \
    --wait --wait-timeout "$timeout_seconds" $wait_services
else
  compose up --detach --no-deps --wait --wait-timeout "$timeout_seconds" $wait_services
fi

if [ "$dashboard" = true ] && [ "$monitoring" = true ]; then
  sh "$script_dir/health.sh" --dashboard --monitoring
elif [ "$dashboard" = true ]; then
  sh "$script_dir/health.sh" --dashboard
elif [ "$monitoring" = true ]; then
  sh "$script_dir/health.sh" --monitoring
else
  sh "$script_dir/health.sh"
fi
