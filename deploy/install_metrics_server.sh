#!/bin/bash
# Installs the hardware metrics server

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt install python3 cron tcpdump iproute2 net-tools -y
# run on startup
(crontab -l 2>/dev/null; echo "@reboot sudo python3 $(pwd)/metrics_server.py > /tmp/metrics_server.log 2>&1") | crontab -
# run now
nohup sudo python3 metrics_server.py > /tmp/metrics_server.log 2>&1 &
