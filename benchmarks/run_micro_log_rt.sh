#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh)
CONFIG="conf/micro_crdv.yaml"
CONFIG_NATIVE="conf/micro_native.yaml"
CONFIG_PG_CRDT="conf/micro_pg_crdt.yaml"
CONFIG_ELECTRIC="conf/micro_electric.yaml"
CONFIG_RIAK="conf/micro_riak.yaml"
TIME=60
RUNS=1
WORKERS=1
STRUCTURES=100
ENTRIES=1
ENGINES="crdv native pg_crdt electric riak"

updateConfig() {
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $STRUCTURES/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: $ENTRIES/" backup.yaml
    sed -i'' 's/weight:.*/weight: 0/' backup.yaml
    awk 'replace {sub("weight: 0", "weight: 1", $0)} {print} {replace=/(registerGet|registerSet)\s*$/}' backup.yaml > backup.yaml.tmp
    mv backup.yaml.tmp backup.yaml
}

run() {
    echo "Running $1"
    updateConfig
    ./benchmarks --conf backup.yaml > out.txt 2> results/micro_log_rt/log_$1.log
    sed -i'' "s/}/,\"type\":\"$1\"}/g" results/micro_log_rt/log_$1.log
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_log_rt

# crdv
if [[ $ENGINES == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    run crdv-sync
    sed -i'' "s/modes:.*/modes: {readMode: all, writeMode: async}/" backup.yaml
    run crdv-async
fi

# native
if [[ $ENGINES == *"native"* ]]; then
    cp $CONFIG_NATIVE backup.yaml
    run native
fi

# pg_crdt
if [[ $ENGINES == *"pg_crdt"* ]]; then
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: remote/" backup.yaml
    run pg_crdt
fi

# electric
if [[ $ENGINES == *"electric"* ]]; then
    cp $CONFIG_ELECTRIC backup.yaml
    run electric
fi

# riak
if [[ $ENGINES == *"riak"* ]]; then
    cp $CONFIG_RIAK backup.yaml
    run riak
fi

# delete the config backup
rm backup.yaml

# plot
python3 plot_log_rt.py results/micro_log_rt/log_*.log -f "operation == 'registerGet'" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -b 1 -c 1 -height 3 -width 3 \
    -ymin 0 -ymax 15.99 -g type -loc "upper left" --no-title \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -markers 'o' '^' 'X' 's' 'P' 'v' \
    -markevery 10 -o results/micro_log_rt/rt_read.png

python3 plot_log_rt.py results/micro_log_rt/log_*.log -f "operation == 'registerSet'" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -b 1 -c 1 -height 3 -width 3 \
    -ymin 0 -ymax 15.99 -g type -loc "upper left" --no-title \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -markers 'o' '^' 'X' 's' 'P' 'v' \
    -markevery 10 -o results/micro_log_rt/rt_write.png
