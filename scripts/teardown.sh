#!/bin/sh
set -eu

remove_volumes=false
while [ "$#" -gt 0 ]; do
  case "$1" in
    --volumes)
      remove_volumes=true
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
  echo ".env is missing. Nothing was changed." >&2
  exit 1
fi

if [ "$remove_volumes" = true ]; then
  docker compose \
    --env-file "$env_file" \
    --file "$compose_file" \
    --profile dashboard \
    --profile monitoring \
    down --remove-orphans --volumes
  echo "Millrace containers and local volumes were removed."
else
  docker compose \
    --env-file "$env_file" \
    --file "$compose_file" \
    --profile dashboard \
    --profile monitoring \
    down --remove-orphans
  echo "Millrace containers were removed; local volumes were retained."
fi
