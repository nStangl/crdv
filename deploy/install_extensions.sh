#!/bin/bash

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y make gcc postgresql-server-dev-16 libkrb5-dev git

# pg_background
git clone https://github.com/vibhorkum/pg_background
cd pg_background
make
sudo make install

# clocks and list ids
cd ../../schema/clocks
make
sudo make install
cd ../list_ids
make
sudo make install
