#!/bin/bash

cron
/main/deploy/riak/run.sh 1
/main/deploy/riak/create-bucket-types.sh 1
bash
