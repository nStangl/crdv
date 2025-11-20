#!/bin/bash
set -e
cron

pg_ctlcluster 16 main start

until pg_isready -h localhost -p 5432 > /dev/null 2>&1; do
  echo "Waiting for Postgres to be ready..."
  sleep 1
done

SCHEMA_DIR="schema/sql"
MAX_HOPS=${MAX_HOPS:-1}

execute_sql_files() {
  local folder="$1"
  for file in $(find "$folder" -type f -name "*.sql" ! -name "00-drop.sql" | sort); do
    sed "s/{{MAX_HOPS}}/$MAX_HOPS/g" "$file" | psql -q -v ON_ERROR_STOP=1 -U postgres -d testdb
  done
}

execute_sql_files "$SCHEMA_DIR"

pg_ctlcluster 16 main stop

exec pg_ctlcluster 16 main start --foreground
