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
WORKERS=1
STRUCTURES=100000
ENTRIES=100
ENGINES="crdv native pg_crdt electric riak"

# if an argument is provided, use it as the engine
if [[ -n $1 ]]; then
    ENGINES_="$1"
else
    ENGINES_="$ENGINES"
fi

updateConfig() {
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
    sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $STRUCTURES/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: $ENTRIES/" backup.yaml
}

run() {
    echo "Running $1"
    updateConfig
    ./benchmarks --conf backup.yaml --no-log > out.txt
    grep -Po "(?<=CsvOps:).*" out.txt > results/micro/results_$1.csv
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro

# crdv
if [[ $ENGINES_ == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    run sync
    sed -i'' "s/modes:.*/modes: {readMode: all, writeMode: async}/" backup.yaml
    run async
fi

# native
if [[ $ENGINES_ == *"native"* ]]; then
    cp $CONFIG_NATIVE backup.yaml
    run native
fi

# pg_crdt
if [[ $ENGINES_ == *"pg_crdt"* ]]; then
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: remote/" backup.yaml
    run pg_crdt
fi

# electric
if [[ $ENGINES_ == *"electric"* ]]; then
    cp $CONFIG_ELECTRIC backup.yaml
    run electric
fi

# riak
if [[ $ENGINES_ == *"riak"* ]]; then
    cp $CONFIG_RIAK backup.yaml
    run riak
fi

# delete the config backup
rm backup.yaml

# plot
python3 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('counter', '')" -f "operation.str.startswith('counter')" \
    -g "engine" -height 2.9 -xname "" -yname "Response time (ms)" -ymax 7 \
    -xorder Get Inc Dec -gorder crdv-sync crdv-async native pg_crdt riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#D85343" -p "rtP95 * 1000" -plabel "$\it{p95}$" \
    -hatches '///' '+++' '' '___' '\\\' -o results/micro/counter.png 2> /dev/null

python3 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('register', '')" -f "operation.str.startswith('register')" \
    -g "engine" -height 2.9 -xname "" -yname "Response time (ms)" -ymax 7 \
    -xorder Get Set -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -p "rtP95 * 1000" -plabel "$\it{p95}$" \
    -hatches '///' '+++' '' '___' '|||' '\\\' -o results/micro/register.png 2> /dev/null

python3 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('set', '')" -f "operation.str.startswith('set')" \
    -g "engine" -height 2.9 -xname "" -yname "Response time (ms)" -ymax 7 \
    -xorder Get Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -p "rtP95 * 1000" -plabel "$\it{p95}$" \
    -hatches '///' '+++' '' '___' '|||' '\\\' -o results/micro/set.png 2> /dev/null

python3 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('map', '')" -f "operation.str.startswith('map')" \
    -g "engine" -height 2.9 -xname "" -yname "Response time (ms)" -ymax 7 \
    -xorder Get Value Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -p "rtP95 * 1000" -plabel "$\it{p95}$" \
    -hatches '///' '+++' '' '___' '|||' '\\\' -o results/micro/map.png 2> /dev/null

python3 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('list', '')" -f "operation.str.startswith('list')" \
    -g "engine" -height 2.9 -xname "" -yname "Response time (ms)" -ymax 7 \
    -xorder Get GetAt Add Append Prepend Rmv -gorder crdv-sync crdv-async native pg_crdt \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" -p "rtP95 * 1000" -plabel "$\it{p95}$" \
    -hatches '///' '+++' '' '___' -o results/micro/list.png 2> /dev/null
