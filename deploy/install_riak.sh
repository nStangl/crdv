#!/bin/bash

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y make libc6-dev-i386 git curl unzip libncurses5-dev libssl-dev cmake gcc g++ libpam0g-dev wget python3 python3-requests
git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.13.1
. "$HOME/.asdf/asdf.sh"
asdf plugin-add erlang https://github.com/asdf-vm/asdf-erlang.git
asdf install erlang 25.3
asdf global erlang 25.3
wget https://github.com/basho/riak/archive/refs/tags/riak-3.2.0.zip
unzip riak-3.2.0.zip
mv riak-riak-3.2.0 riak
cd riak
sed -i -E "s/ERLANG_BIN[[:space:]]+=.*/ERLANG_BIN = erl +c false/" Makefile
sed -i -E "s/REBAR[[:space:]]+\?=.*/REBAR ?= ERL_FLAGS=\"+c false\" \$(BASE_DIR)\/rebar3/" Makefile
make rel

# by default, bitcask does not sync the data to disk, even when explicitly setting in the config,
# due to a bug in the code. the following commands fix that bug.
sed -i "s/\[o_sync | Opts\];/Opts ++ [o_sync];/" _build/rel/lib/bitcask/src/bitcask_fileops.erl
sed -i "s/HintFD = open_hint_file(Filename, FinalOpts)/HintFD = open_hint_file(Filename, Opts0)/" _build/rel/lib/bitcask/src/bitcask_fileops.erl
(cd _build/rel/lib/bitcask; make clean; rm -r ebin/*)
make rel


# server to measure the data size on disk (listening on port 8081)
cat <<EOF > storage.py
from http.server import HTTPServer, BaseHTTPRequestHandler
from socketserver import ThreadingMixIn
import os
from subprocess import Popen, check_output 
import time
import re
import requests
import threading

class CustomHandler(BaseHTTPRequestHandler):
    def success(self, msg):
        self.send_response(200)
        self.send_header('Content-type', 'text/plain')
        self.end_headers()
        self.wfile.write(msg.encode())

    def reset(self, ip):
        requests.post(f"http://{ip}:8081/reset")

    def resetCluster(self, ip):
        requests.post(f"http://{ip}:8081/resetCluster")

    def do_POST(self):
        if self.path == '/resetAll':
            threads = []

            # send the resetCluster command to all clusters, including the current one
            # (when using cluster-cluster replication)
            connections = check_output(['./repl-connections.sh']).decode("utf-8")
            for ip in re.findall(r'via (.*?):', connections) + ["localhost"]:
                t = threading.Thread(target=self.resetCluster, args=(ip,))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self.success("ok")

        elif self.path == '/resetCluster':
            threads = []
            
            # send the reset command to the members of the current cluster
            status = check_output(['./status.sh']).decode("utf-8")

            for ip in re.findall(r'\w+@(.*?) ', status):
                t = threading.Thread(target=self.reset, args=(ip,))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

            self.success("ok")

        elif self.path == '/reset':
            Popen(['./reset.sh']).wait()
            self.success("ok")

        else:
            size = 0
            try:
                for node in os.listdir('rel/riak/data/bitcask/'):
                    for file in os.listdir(os.path.join('rel/riak/data/bitcask/', node)):
                        if file.endswith('.bitcask.data'):
                            size += os.path.getsize(os.path.join('rel/riak/data/bitcask/', node, file))
            except:
                pass
            self.success(str(size))

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

if __name__ == '__main__':
    server = ThreadedHTTPServer(('0.0.0.0', 8081), CustomHandler)
    server.serve_forever()
EOF

# start a node script
cat <<EOF > run.sh
#!/bin/bash
# Starts a node.
# When taking a node down and bringing it back up, we must ensure that the id and ip remain the same. 

if ! [ "\$#" -eq 1 ] && ! [ "\$#" -eq 2 ]; then
    echo "Usage: run.sh <node-id> [node-ip]."
    echo "If no node-ip is provided, the IP address returned by 'hostname -i' will be used."
    exit 1
fi

IP=\$2
if [ -z "\$IP" ]; then
    IP=\$(hostname -i)
fi

echo "Starting node \$1@\$IP"

ps aux | grep "[s]torage.py" > /dev/null
if [ \$? -ne 0 ]; then
    nohup python3 storage.py > storage.log 2>&1 &
fi

. "$HOME/.asdf/asdf.sh"
sed -i "s/listener.http.internal =.*/listener.http.internal = \$IP:8098/g" rel/riak/etc/riak.conf
sed -i "s/listener.protobuf.internal =.*/listener.protobuf.internal = \$IP:8087/g" rel/riak/etc/riak.conf
sed -i "s/nodename = .*/nodename = riak\$1@\$IP/g" rel/riak/etc/riak.conf
sed -i "s/## protobuf.backlog =.*/protobuf.backlog = 1000/" rel/riak/etc/riak.conf
sed -i "s/{rtq_max_bytes, .*}/{rtq_max_bytes, 1048576000}/" rel/riak/etc/advanced.config
sed -i "s/127.0.0.1/0.0.0.0/" rel/riak/etc/advanced.config
grep -qxF "bitcask.io_mode = nif" rel/riak/etc/riak.conf || echo "bitcask.io_mode = nif" >> rel/riak/etc/riak.conf
grep -qxF "bitcask.sync.strategy = o_sync" rel/riak/etc/riak.conf || echo "bitcask.sync.strategy = o_sync" >> rel/riak/etc/riak.conf
rel/riak/bin/riak start
EOF

# join a node script (same cluster)
cat <<EOF > join.sh
#!/bin/bash
# Joins the local node to a cluster.

if [ "\$#" -ne 2 ]; then
    echo "Usage: join.sh <id-node-to-join> <ip-node-to-join>"
    exit 1
fi

. "$HOME/.asdf/asdf.sh"
while rel/riak/bin/riak-admin cluster join riak\$1@\$2; [ ! \$? -eq 0 ]; do
    sleep 1
done
rel/riak/bin/riak-admin cluster plan
rel/riak/bin/riak-admin cluster commit
rel/riak/bin/riak-admin cluster status
EOF

# cluster status script
cat <<EOF > status.sh
#!/bin/bash
# Checks the status of the current cluster

. "$HOME/.asdf/asdf.sh"
out=\$(rel/riak/bin/riak-admin cluster status)
printf "%s\n" "\$out"
if [[ \$out == *"is not responding to pings"* ]]; then
    exit 1
else
    exit 0
fi
EOF

# create the bucket types script
cat <<EOF > create-bucket-types.sh
#!/bin/bash
# Creates the required bucket types.
# Receives n_val as argument (in how many different nodes are data stored).

if [ "\$#" -ne 1 ]; then
    echo "Usage: create-bucket-types.sh <n_val>"
    exit 1
fi

rel/riak/bin/riak-admin bucket-type create counters '{"props":{"datatype":"counter", "r": 1, "w": 1, "rw": 1, "dw": 1, "n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type activate counters

rel/riak/bin/riak-admin bucket-type create sets '{"props":{"datatype":"set", "r": 1, "w": 1, "rw": 1, "dw": 1, "n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type activate sets

rel/riak/bin/riak-admin bucket-type create maps '{"props":{"datatype":"map", "r": 1, "w": 1, "rw": 1, "dw": 1, "n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type activate maps

rel/riak/bin/riak-admin bucket-type create registers '{"props":{"r": 1, "w": 1, "rw": 1, "dw": 1, "n_val": '\$1', "allow_mult": false, "dvv_enabled": false, "last_write_wins": true}}'
rel/riak/bin/riak-admin bucket-type activate registers
EOF

# update the bucket types script
cat <<EOF > update-bucket-types.sh
#!/bin/bash
# Updates the bucket types with a new n_val.

if [ "\$#" -ne 1 ]; then
    echo "Usage: update-bucket-types.sh <n_val>"
    exit 1
fi

rel/riak/bin/riak-admin bucket-type update counters '{"props":{"n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type update sets '{"props":{"n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type update maps '{"props":{"n_val": '\$1'}}'
rel/riak/bin/riak-admin bucket-type update registers '{"props":{"n_val": '\$1'}}'
EOF


# remove a node from the cluster script
cat <<EOF > remove.sh
#!/bin/bash
# Removes a node from the current cluster.

if [ "\$#" -ne 2 ]; then
    echo "Usage: remove.sh <id-node-to-remove> <ip-node-to-remove>"
    exit 1
fi

. "$HOME/.asdf/asdf.sh"

rel/riak/bin/riak-admin cluster leave riak\$1@\$2
rel/riak/bin/riak-admin cluster plan
rel/riak/bin/riak-admin cluster commit
rel/riak/bin/riak-admin cluster status
EOF


# prepare the cluster for cluster-cluster replication
cat <<EOF > repl-prepare.sh
#!/bin/bash
# Prepare the cluster for cluster-cluster replication (V3 Multi-Datacenter Replication)

if [ "\$#" -ne 1 ]; then
    echo "Usage: prepare-cluster.sh <clustername>"
    exit 1
fi

. "$HOME/.asdf/asdf.sh"

rel/riak/bin/riak-repl clustername \$1
EOF


# connect this cluster to another
cat <<EOF > repl-connect.sh
#!/bin/bash
# Connect this cluster to another, to replicate data to it.
# For bi-directional replication, this command must be executed on both clusters.
# Ensure both clusters have the same datatypes (create-bucket-types.sh)

if [ "\$#" -ne 2 ]; then
    echo "Usage: connect-cluster.sh <other_clustername> <other_ip>"
    exit 1
fi

self_name=\$(rel/riak/bin/riak-repl clustername)

if [ "\$self_name" = "undefined" ]; then
    echo "Current cluster not yet prepared."
    exit 1
fi

. "$HOME/.asdf/asdf.sh"

rel/riak/bin/riak-repl connect \$2:9080
rel/riak/bin/riak-repl connections
rel/riak/bin/riak-repl realtime enable \$1
rel/riak/bin/riak-repl realtime start \$1
EOF

# list the cluster-cluster replication connections
cat <<EOF > repl-connections.sh
#!/bin/bash
# List the cluster-cluster replication connections.

. "$HOME/.asdf/asdf.sh"

out=\$(rel/riak/bin/riak-repl connections)
printf "%s\n" "\$out"

if [[ "\$out" =~ (is not responding to pings|failed) ]]; then
    exit 1
else
    exit 0
fi
EOF


# stops the node and resets all data
cat <<EOF > reset.sh
#!/bin/bash
# Clears the data and restarts the node (the cluster info is maintained).
# This should be done on all nodes, when we want to clear the data.

. "$HOME/.asdf/asdf.sh"
rm -r rel/riak/data/bitcask
> rel/riak/log/erlang.log.1
rel/riak/bin/riak stop
rel/riak/bin/riak start

# wait until the node is ready
grep "Eshell " rel/riak/log/erlang.log.1 > /dev/null
while [ \$? -ne 0 ]; do
    sleep 1
    grep "Eshell " rel/riak/log/erlang.log.1 > /dev/null
done
EOF

# stops the node and resets all data
cat <<EOF > destroy.sh
#!/bin/bash
# Stop this node and clear all data.
# This should be done on all nodes, when we want to destroy the cluster.

. "$HOME/.asdf/asdf.sh"
rel/riak/bin/riak stop
rm -r rel/riak/data
> rel/riak/log/erlang.log.1
sudo pkill -f storage.py
EOF

# check if the node has fully started
cat <<EOF > started.sh
#!/bin/bash
# Check if this node has fully started, by checking the erlang log.

grep "Eshell " rel/riak/log/erlang.log.1 > /dev/null

if [ \$? -eq 0 ]; then
    echo "yes"
    exit 0
else
    echo "no"
    exit 1
fi
EOF


sudo chmod +x *.sh
