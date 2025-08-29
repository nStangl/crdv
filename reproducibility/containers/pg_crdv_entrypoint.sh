#!/bin/bash
set -e
cron

pg_ctlcluster 16 main start

until pg_isready -h localhost -p 5432 > /dev/null 2>&1; do
  echo "Waiting for Postgres to be ready..."
  sleep 1
done

SCHEMA_DIR="schema/sql"

execute_sql_files() {
  local folder="$1"
  for file in $(find "$folder" -type f -name "*.sql" | sort); do
    psql -q -v ON_ERROR_STOP=1 -U postgres -d testdb -f "$file"
  done
}

execute_sql_files "$SCHEMA_DIR"

pg_ctlcluster 16 main stop

exec pg_ctlcluster 16 main start --foreground
