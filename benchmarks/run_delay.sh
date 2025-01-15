#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh for crdv)
CONFIG="conf/delay_crdv.yaml"
CONFIG_PG_CRDT="conf/delay_pg_crdt.yaml"
CONFIG_RIAK="conf/delay_riak.yaml"
TIME=60
WORKERS=(2 4 8 16 32 64)
COUNTERS=100 # per worker
LOG_DELTA=100 # ms
ENGINES="crdv pg_crdt riak"
POST_END_WAIT=10
BUCKET_INTERVAL=1 # seconds
PARTITION_NETWORK=true
PARTITION_NETWORK_START=25 # seconds into the run
PARTITION_NETWORK_DURATION=10 # seconds
PARTITION_NETWORK_SERVER_CRDV="localhost:8083"
PARTITION_NETWORK_SERVER_RIAK="localhost:8083"
PARTITION_NETWORK_SERVER_PG_CRDT="localhost:8083"
BLOCK_IPS_CRDV="ip=localhost"
BLOCK_IPS_RIAK="ip=localhost"
BLOCK_IPS_PG_CRDT="ip=all"

# crdv
MERGE_DELTA=0.05 # seconds
MERGE_BATCH_SIZE=10000
MERGE_PARALLELISM=1


updateConfig() {
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$1]/" backup.yaml
    sed -i'' "s/runs:.*/runs: 1/" backup.yaml
    sed -i'' "s/counters:.*/counters: $COUNTERS/" backup.yaml
    sed -i'' "s/logDelta:.*/logDelta: $LOG_DELTA/" backup.yaml
    sed -i'' "s/postEndWait:.*/postEndWait: $POST_END_WAIT/" backup.yaml
}

partition_network() {
    grep "Running" out.txt > /dev/null
    while [ $? -ne 0 ]; do
        sleep 0.1
        grep "Running" out.txt > /dev/null
    done

    sleep $PARTITION_NETWORK_START
    curl -s -X POST "$1/block?$2&time=$PARTITION_NETWORK_DURATION" > /dev/null
}

run() {
    truncate log.log --size 0

    for workers in "${WORKERS[@]}"; do
        echo "Running $1 $workers"
        updateConfig $workers

        if [ "$PARTITION_NETWORK" = "true" ]; then
            truncate out.txt --size 0
            partition_network $2 $3 &
        fi

        # helps prevent OOM with Pg_crdt
        GOMEMLIMIT=4GiB ./benchmarks --conf backup.yaml 2> log.log > out.txt
        python3 process_delay_log.py log.log -b $BUCKET_INTERVAL -o results/delay/${1}_$workers.csv
        grep -Po "(?<=Csv:).*" out.txt > results/delay/results_${1}_$workers.csv

    done
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/delay

# crdv
if [[ $ENGINES == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    sed -i'' 's/discardUnmergedWhenFinished:.*/discardUnmergedWhenFinished: true/' backup.yaml
    sed -i'' "s/mergeDelta:.*/mergeDelta: $MERGE_DELTA/" backup.yaml
    sed -i'' "s/mergeBatchSize:.*/mergeBatchSize: $MERGE_BATCH_SIZE/" backup.yaml
    sed -i'' "s/mergeParallelism:.*/mergeParallelism: $MERGE_PARALLELISM/" backup.yaml
    run crdv $PARTITION_NETWORK_SERVER_CRDV $BLOCK_IPS_CRDV
fi

# pg_crdt
if [[ $ENGINES == *"pg_crdt"* ]]; then
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: local/" backup.yaml
    sed -i'' "s/replication:.*/replication: operation/" backup.yaml
    run pg_crdt $PARTITION_NETWORK_SERVER_PG_CRDT $BLOCK_IPS_PG_CRDT
fi

# riak
if [[ $ENGINES == *"riak"* ]]; then
    cp $CONFIG_RIAK backup.yaml
    sed -i'' "s/reset:.*/reset: true/" backup.yaml
    run riak $PARTITION_NETWORK_SERVER_RIAK $BLOCK_IPS_RIAK
fi

# delete the config backup
rm backup.yaml

for engine in $ENGINES; do
    python3 plot_line.py results/delay/${engine}_*.csv \
        -x time -y delay -xname "Time (seconds)" -yname "Delay (missing operations)" \
        -g "workers" -height 3 -width 3 --log \
        -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
        -o results/delay/delay_$engine.png

    python3 plot_line.py results/delay/${engine}_*.csv \
        -x time -y tps -xname "Time (seconds)" -yname "Throughput (tx/s)" \
        -g "workers" -height 3 -width 3 --log \
        -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
        -o results/delay/tps_$engine.png
done
