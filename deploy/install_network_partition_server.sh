#!/bin/bash
# Installs the network partition server

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt install python3 cron iproute2 net-tools -y
# run on startup
(crontab -l 2>/dev/null; echo "@reboot sudo python3 $(pwd)/network_partition_server.py > /tmp/network_partition_server.log 2>&1") | crontab -
# run now
nohup sudo python3 network_partition_server.py > /tmp/network_partition_server.log 2>&1 &
