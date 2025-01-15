#!/bin/bash

sudo DEBIAN_FRONTEND=noninteractive apt install -y unzip git curl gcc make libssl-dev libncurses5-dev wget
git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.13.1
. "$HOME/.asdf/asdf.sh"
asdf plugin-add erlang https://github.com/asdf-vm/asdf-erlang.git
asdf install erlang 25.3
asdf plugin-add elixir https://github.com/asdf-vm/asdf-elixir.git
asdf install elixir 1.15-otp-25
# in July 2024 the electric sql repo got updated, so we must use the old one 
wget https://github.com/electric-sql/electric-old/archive/refs/tags/@core/electric@0.9.0.zip
unzip electric\@0.9.0.zip
mv electric-old--core-electric-0.9.0/ electric
cd electric/components/electric/
mix local.hex --force
mix deps.get
MIX_ENV="prod" elixir --erl "+c false" -S mix release
cd ../../

# start service script
cat <<EOF > run.sh
#!/bin/bash
# Starts the electric service and creates the requires tables.

DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric \
    LOGICAL_PUBLISHER_HOST=localhost \
    LOGICAL_PUBLISHER_PORT=5434 \
    PG_PROXY_PASSWORD=postgres \
    AUTH_MODE=insecure \
    nohup ./components/electric/_build/prod/rel/electric/bin/electric start > electric.log 2>&1 &

while psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "select 1" &> /dev/null; [ ! \$? -eq 0 ]; do
    echo Waiting for the service to start
    sleep 1
done

# drop tables if they exist
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_register" &> /dev/null
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_set" &> /dev/null
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_map" &> /dev/null
# create tables required by the benchmark
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "create table electric_register(id varchar primary key, value varchar)"
# the 'unused' variable is required as it appears that we must have a non-primary key column
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "create table electric_set(id varchar, elem varchar, unused varchar, primary key(id, elem))"
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "create table electric_map(id varchar, key varchar, value varchar, primary key(id, key))"
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "alter table electric_register enable electric"
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "alter table electric_set enable electric"
psql postgresql://postgres:postgres@127.0.0.1:65432/testdb_electric -c "alter table electric_map enable electric"

ps aux | grep "[s]chema.py" > /dev/null
if [ \$? -ne 0 ]; then
    nohup python3 schema.py > schema.log 2>&1 &
fi
EOF

# stop service script
cat <<EOF > stop.sh
#!/bin/bash
# Stops the electric service.

psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "select pg_drop_replication_slot('electric_replication_out_testdb_electric');"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "alter subscription postgres_1 disable;"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "alter subscription postgres_1 set (slot_name = NONE);"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop subscription postgres_1 ;"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop publication electric_publication;"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop schema electric cascade"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_register"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_set"
psql postgresql://postgres:postgres@127.0.0.1:5432/testdb_electric -c "drop table if exists electric_map"
./components/electric/_build/prod/rel/electric/bin/electric stop

ps aux | grep "[b]eam\.smp" > /dev/null
while [ \$? -eq 0 ]; do
    sleep 1
    ps aux | grep "[b]eam\.smp" > /dev/null
done

stty sane # so the shell keeps working correctly
EOF

# server to reset the schema (listening on port 8082)
cat <<EOF > schema.py
from http.server import HTTPServer, BaseHTTPRequestHandler
import os
from subprocess import Popen 
import time

class CustomHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        Popen(['./stop.sh']).wait()
        Popen(['./run.sh']).wait()
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write("ok".encode())

server = HTTPServer(('0.0.0.0', 8082), CustomHandler)
server.serve_forever()
EOF

sudo chmod +x *.sh
