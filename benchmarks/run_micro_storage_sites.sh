#!/bin/bash

if [ $# -eq 0 ]; then
    echo "Missing engine name. Usage: ./run_micro_storage_sites.sh (crdv|native|electric|riak)"
    exit 1
fi

# database(s) should already exist (see schema/create_cluster.sh for crdv)
CONFIG="conf/micro_crdv.yaml"
CONFIG_NATIVE="conf/micro_native.yaml"
CONFIG_PG_CRDT="conf/micro_pg_crdt.yaml"
CONFIG_ELECTRIC="conf/micro_electric.yaml"
CONFIG_RIAK="conf/micro_riak.yaml"
STRUCTURES=100000 # total single-key maps (maps to use MVR registers with Riak)
ENGINE=$1
VALUE_LENGTH=130 # so native matches the tpc-c average (~210 bytes per row)
MAX_SITES=10 # number of data points generated for native/electric

updateConfig() {
    sed -i'' "s/time:.*/time: 0/" backup.yaml
    sed -i'' "s/vacuumFull:.*/vacuumFull: true/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $STRUCTURES/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: 1/" backup.yaml # one entry per map
    sed -i'' "s/typesToPopulate:.*/typesToPopulate: [map]/" backup.yaml
    sed -i'' "s/valueLength:.*/valueLength: $VALUE_LENGTH/" backup.yaml

    sed -i'' 's/weight:.*/weight: 0/' backup.yaml
    awk 'replace {sub("weight: 0", "weight: 1", $0)} {print} {replace=/(mapAdd)\s*$/}' backup.yaml > backup.yaml.tmp
    mv backup.yaml.tmp backup.yaml
}

# crdv: perform one run per cluster size (cluster must be already deployed)
# native/electric: only one run needed
# riak: one run needed; cluster with MAX_SITES sites must already be deployed
run() {
    echo "Running $1"
    updateConfig

    if [[ $ENGINE == "riak" ]]; then
        # first run to clear the data
        sed -i'' "s/reset:.*/reset: true/" backup.yaml
        ./benchmarks --conf backup.yaml --no-log > out.txt
        sed -i'' "s/reset:.*/reset: false/" backup.yaml

        # populate with each site to incrementally build the complete vector clock;
        # the run computes the difference to the previous state to get the true storage size
        for i in $(seq 0 $(expr $MAX_SITES - 1)); do
            sites=$(expr $i + 1)
            echo "  riak-$sites"
            sed -i'' "s/populateClient:.*/populateClient: $i/" backup.yaml
            ./benchmarks --conf backup.yaml --no-log > out.txt
            grep -Po "(?<=Csv:).*" out.txt > results/micro_storage_sites/results_$1_$sites.csv
            sed -i'' "s/,[[:digit:]]\+,riak,/,$sites,riak,/" results/micro_storage_sites/results_riak_$sites.csv
        done
    else
        ./benchmarks --conf backup.yaml --no-log > out.txt
        sites=$(grep -Po '(?<=sites: )\d+' out.txt)
        grep -Po "(?<=Csv:).*" out.txt > results/micro_storage_sites/results_$1_$sites.csv
        sed -i'' s/crdv-sync/crdv/ results/micro_storage_sites/results_$1_$sites.csv
    fi
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_storage_sites

# crdv
if [[ $ENGINE == "crdv" ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    run crdv
fi

# native
if [[ $ENGINE == "native" ]]; then
    cp $CONFIG_NATIVE backup.yaml
    run native
fi

# electric
if [[ $ENGINE == "electric" ]]; then
    cp $CONFIG_ELECTRIC backup.yaml
    sed -i'' "s/reset:.*/reset: true/" backup.yaml
    run electric
fi

# riak
if [[ $ENGINE == "riak" ]]; then
    cp $CONFIG_RIAK backup.yaml
    run riak
fi

# delete the config backup
rm backup.yaml

# generate the extra data points (both native and electric only have one site)
if [[ $ENGINE == "native" ]] || [[ $ENGINE == "electric" ]]; then
    for i in $(seq 2 $MAX_SITES); do
        cp results/micro_storage_sites/results_${ENGINE}_1.csv results/micro_storage_sites/results_${ENGINE}_$i.csv
        sed -i'' "s/,1,$ENGINE,/,$i,$ENGINE,/" results/micro_storage_sites/results_${ENGINE}_$i.csv
    done
fi

python3 plot_line.py results/micro_storage_sites/results_*.csv \
    -y "startSize / 1e6" -x "sites" -t \
    -xname "Number of sites" -yname "Storage used (MB)" -g "engine" -height 3 -width 3 \
    -gorder crdv native electric riak -ncols 2 \
    -colors "#034078" "#0A1128" "#DF7E20" "#D85343" \
    -dashes '(10000, 1)' '(2, 1)' '(2, 1)' '(10000, 1)' \
    -markers 'o' 'X' 'P' 'v' -o results/micro_storage_sites/storage.png
