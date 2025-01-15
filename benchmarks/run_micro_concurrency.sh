#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh for crdv)
CONFIG="conf/micro_crdv.yaml"
CONFIG_NATIVE="conf/micro_native.yaml"
CONFIG_PG_CRDT="conf/micro_pg_crdt.yaml"
CONFIG_ELECTRIC="conf/micro_electric.yaml"
CONFIG_RIAK="conf/micro_riak.yaml"
TIME=60
WARMUP=3
COOLDOWN=3
RUNS=3
WORKERS=64
ENTRIES=(1 2 4 8 16 32 64 128 256 512 1024)
ENGINES="crdv native pg_crdt electric riak"

updateConfig() {
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
    sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/typesToPopulate:.*/typesToPopulate: [map]/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: 1/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: $1/" backup.yaml
    sed -i'' 's/weight:.*/weight: 0/' backup.yaml
    awk 'replace {sub("weight: 0", "weight: 1", $0)} {print} {replace=/(mapAdd)\s*$/}' backup.yaml > backup.yaml.tmp
    mv backup.yaml.tmp backup.yaml
}

run() {
    for entries in "${ENTRIES[@]}"; do
        echo "Running $1 entries=$entries"
        updateConfig $entries
        ./benchmarks --conf backup.yaml --no-log > out.txt
        grep -Po "(?<=Csv:).*" out.txt > results/micro_concurrency/results_${1}_$entries.csv
    done
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_concurrency

# crdv
if [[ $ENGINES == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/discardUnmergedWhenFinished:.*/discardUnmergedWhenFinished: true/" backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    run sync
    sed -i'' "s/modes:.*/modes: {readMode: all, writeMode: async}/" backup.yaml
    run async
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

python3 plot_line.py results/micro_concurrency/results_*.csv \
    -y "tps" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Throughput (txn/s)" -g "engine" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -height 3 -width 3 -xbins 6 -kxticks -kyticks \
    -o results/micro_concurrency/tps.png

python3 plot_line.py results/micro_concurrency/results_*.csv \
    -y "rt * 1000" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Response time (ms)" -g "engine" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -height 3 -width 3 --log -xbins 6 -kxticks \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" \
    -o results/micro_concurrency/rt.png
