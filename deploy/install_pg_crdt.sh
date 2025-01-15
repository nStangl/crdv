#!/bin/bash

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y cargo pkg-config libreadline-dev zlib1g-dev libclang-dev postgresql-server-dev-15 rustfmt make git
sudo cargo install --locked cargo-pgx@^0.6
sudo cargo pgx init --pg15 /usr/lib/postgresql/15/bin/pg_config
git clone https://github.com/supabase/pg_crdt.git
cd pg_crdt
sed -i 's/automerge =.*/automerge = "0.5.0"/' Cargo.toml
sudo cargo pgx install -c /usr/lib/postgresql/15/bin/pg_config
PGPASSWORD=postgres psql -U postgres -p 5433 -d testdb -c "create extension pg_crdt"
