#!/bin/bash
set -e
cron
service postgresql start

until pg_isready -h localhost -p 5432 > /dev/null 2>&1; do
  echo "Waiting for Postgres to be ready..."
  sleep 1
done

SCHEMA_DIR="schema/sql"

execute_sql_files() {
  local folder="$1"
  for file in $(find "$folder" -type f -name "*.sql" | sort); do
    echo "Executing $file"
    psql -U postgres -d testdb -f "$file"
  done
}

execute_sql_files "$SCHEMA_DIR"

if [ "$1" != "" ]; then
 exec "$@"
else
  exec pg_ctlcluster 16 main start --foreground
fi