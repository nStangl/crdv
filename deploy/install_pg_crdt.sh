#!/bin/bash

sudo apt-get update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y pkg-config libreadline-dev zlib1g-dev libclang-dev postgresql-server-dev-15 make git curl gcc
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
sudo /root/.cargo/bin/cargo install --locked cargo-pgx@^0.6
sudo /root/.cargo/bin/cargo pgx init --pg15 /usr/lib/postgresql/15/bin/pg_config
git clone https://github.com/supabase/pg_crdt.git
cd pg_crdt
git checkout f6020076225c5b3073cfe623bfa4bf78982c53bf # version used in the paper
sed -i 's/automerge =.*/automerge = "0.5.0"/' Cargo.toml
sudo /root/.cargo/bin/cargo pgx install -c /usr/lib/postgresql/15/bin/pg_config
PGPASSWORD=postgres psql -U postgres -p 5433 -d testdb -c "create extension pg_crdt"
