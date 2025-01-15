#!/bin/bash

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt install -y wget
wget https://dl.google.com/go/go1.21.6.linux-amd64.tar.gz
sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.21.6.linux-amd64.tar.gz
echo export PATH=\$PATH:/usr/local/go/bin >> $HOME/.profile
source $HOME/.profile
go version # should output go version go1.21.6 linux/amd64
