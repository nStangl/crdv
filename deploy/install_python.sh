#!/bin/bash

sudo apt update
sudo DEBIAN_FRONTEND=noninteractive apt install -y python3 python3-pip
pip3 install -r requirements.txt
