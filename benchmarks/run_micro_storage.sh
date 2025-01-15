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
TOTAL_ENTRIES=100000 # total entries
ENTRIES_PER_STRUCTURE=(1 2 4 8 16 32 64 128)
ENGINES="crdv native pg_crdt electric riak"

updateConfig() {
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
    sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/vacuumFull:.*/vacuumFull: true/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $1/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: $2/" backup.yaml
    sed -i'' "s/typesToPopulate:.*/typesToPopulate: [map]/" backup.yaml
    sed -i'' "s/reset:.*/reset: true/" backup.yaml # riak and electric only
    sed -i'' "s/noReload:.*/noReload: false/" backup.yaml
    sed -i'' "s/valueLength:.*/valueLength: 4/" backup.yaml

    sed -i'' 's/weight:.*/weight: 0/' backup.yaml
    awk 'replace {sub("weight: 0", "weight: 1", $0)} {print} {replace=/(mapAdd|mapValue)\s*$/}' backup.yaml > backup.yaml.tmp
    mv backup.yaml.tmp backup.yaml
}

run() {
    i=0

    for entries in "${ENTRIES_PER_STRUCTURE[@]}"; do
        structures=$(expr $TOTAL_ENTRIES / $entries)

        echo "Running $1 structures=$structures entries=$entries"
        updateConfig $structures $entries
        ./benchmarks --conf backup.yaml --no-log > out.txt

        # first test, reset the file
        if [ "$i" -eq 0 ]; then
            grep -Po "(?<=Csv:).*" out.txt > results/micro_storage/results_$1.csv
            grep -Po "(?<=CsvOps:).*" out.txt > results/micro_storage/ops_$1.csv
        # otherwise, ignore the header and append to the file
        else
            grep -Po "(?<=Csv:).*" out.txt | tail -n +2 >> results/micro_storage/results_$1.csv
            grep -Po "(?<=CsvOps:).*" out.txt | tail -n +2 >> results/micro_storage/ops_$1.csv
        fi

        ((i++))
    done
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_storage

# crdv
if [[ $ENGINES == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
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
# (riak tests should start with a clean database, as the only way to measure its space usage is to
# measure the size of the bitcask files)
if [[ $ENGINES == *"riak"* ]]; then
    cp $CONFIG_RIAK backup.yaml
    run riak
fi

# delete the config backup
rm backup.yaml

python3 plot_line.py results/micro_storage/results_*.csv \
    -y "startSize / 1e6" -x "initialOpsPerStructure" -t \
    -xname "Map size (# entries)" -yname "Storage used (MB)" -g "engine" -height 3 -width 3 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper right" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o results/micro_storage/storage.png

python3 plot_line.py results/micro_storage/ops_*.csv -f "operation == 'mapValue'" \
    -y "rt * 1000" -x "initialOpsPerStructure" -t -ymax 12 \
    -g "engine" -height 3 -width 3 -xname "Map size (# entries)" -yname "Response time (ms)" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper right" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o results/micro_storage/rt_read.png

python3 plot_line.py results/micro_storage/ops_*.csv -f "operation == 'mapAdd'" \
    -y "rt * 1000" -x "initialOpsPerStructure" -t -ymax 12 \
    -g "engine" -height 3 -width 3 -xname "Map size (# entries)" -yname "Response time (ms)" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -loc "upper left" -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper right" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o results/micro_storage/rt_write.png
