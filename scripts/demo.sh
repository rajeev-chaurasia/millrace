#!/bin/sh
set -eu

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

env_value() {
  awk -F= -v name="$1" \
    '$1 == name { print substr($0, index($0, "=") + 1); found=1; exit }
     END { if (!found) exit 1 }' \
    "$env_file"
}

sh "$script_dir/health.sh"

postgres_user=$(env_value POSTGRES_USER)
postgres_database=$(env_value POSTGRES_DB)

compose exec -T postgres \
  psql --username "$postgres_user" --dbname "$postgres_database" <<'SQL'
\set ON_ERROR_STOP on
SELECT batch_id, control.apply_demo_batch(batch_id) AS applied
FROM generate_series(1, 3) AS batches(batch_id);

DO $$
BEGIN
    IF (SELECT count(*) FROM retail.customers) <> 3
        OR (SELECT count(*) FROM retail.products) <> 4
        OR (SELECT count(*) FROM retail.orders) <> 3
        OR (SELECT count(*) FROM retail.order_items) <> 6
        OR (SELECT count(*) FROM control.source_batch WHERE state = 'completed') <> 3
    THEN
        RAISE EXCEPTION 'deterministic demo validation failed';
    END IF;
END;
$$;

SELECT batch_id, state, row_count, checksum
FROM control.source_batch
ORDER BY batch_id;

SELECT 'customers' AS entity, count(*) AS current_rows FROM retail.customers
UNION ALL SELECT 'products', count(*) FROM retail.products
UNION ALL SELECT 'orders', count(*) FROM retail.orders
UNION ALL SELECT 'order_items', count(*) FROM retail.order_items
ORDER BY entity;
SQL

echo "Deterministic demo batches are loaded and validated."
