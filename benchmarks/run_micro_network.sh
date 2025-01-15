#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh for crdv)
CONFIG="conf/micro_crdv.yaml"
CONFIG_RIAK="conf/micro_riak.yaml"
CONFIG_PG_CRDT="conf/micro_pg_crdt.yaml"
TRANSACTIONS=100000
RUNS=1
WORKERS=1
STRUCTURES=100000
ENGINES="crdv riak pg_crdt"
# must be already running (see deploy/install_metrics_server.sh)
METRICS_SERVER_CRDV="localhost:8080"
METRICS_SERVER_RIAK="localhost:8080"
METRICS_SERVER_PG_CRDT="localhost:8080"
TYPES="map"

declare -A ARGS
# op_type | entries | name
ARGS[register]="\
registerGet|1|Get
registerSet|1|Set"
ARGS[counter]="\
counterGet|1|Get
counterInc|1|Inc"
ARGS[set]="\
setGet|1|$\\\mathregular{Get_1}$
setGet|100|$\\\mathregular{Get_{100}}$
setContains|1|$\\\mathregular{Contains_1}$
setContains|100|$\\\mathregular{Contains_{100}}$
setAdd|1|$\\\mathregular{Add_1}$
setAdd|100|$\\\mathregular{Add_{100}}$"
ARGS[map]="\
mapGet|1|$\\\mathregular{Get_1}$
mapGet|100|$\\\mathregular{Get_{100}}$
mapValue|1|$\\\mathregular{Value_1}$
mapValue|100|$\\\mathregular{Value_{100}}$
mapAdd|1|$\\\mathregular{Add_1}$
mapAdd|100|$\\\mathregular{Add_{100}}$"


updateConfig() {
    sed -i'' "s/time:.*/time: 0/" backup.yaml
    sed -i'' "s/transactions:.*/transactions: $TRANSACTIONS/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $STRUCTURES/" backup.yaml
    sed -i'' "s/typesToPopulate:.*/typesToPopulate: [$1]/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: $3/" backup.yaml
    sed -i'' "s/reset:.*/reset: true/" backup.yaml
    sed -i'' 's/weight:.*/weight: 0/' backup.yaml
    awk 'replace {sub("weight: 0", "weight: 1", $0)} {print} {replace=/('$2').*\s*$/}' backup.yaml > backup.yaml.tmp
    mv backup.yaml.tmp backup.yaml
}

run() {
    for type in "${TYPES[@]}"; do
        while IFS= read -r arg; do
            IFS='|' read -r op_type entries name <<< "$arg"
            echo "Running $1 $type $op_type $entries"
            updateConfig $type $op_type $entries

            curl -X POST $2/start -s > /dev/null
            ./benchmarks --conf backup.yaml --no-log > out.txt
            curl -X POST $2/stop -s > results/micro_network/network_${1}_${type}_${op_type}_${entries}.csv
            grep -Po "(?<=Csv:).*" out.txt > results/micro_network/results_${1}_${type}_${op_type}_${entries}.csv
            grep -Po "(?<=CsvOps:).*" out.txt > results/micro_network/ops_${1}_${type}_${op_type}_${entries}.csv

            setupTime=$(grep -Po "(?<=setupTime=).*" out.txt)

            python3 -c \
"import pandas as pd; \
df = pd.read_csv('results/micro_network/network_${1}_${type}_${op_type}_${entries}.csv'); \
df = df[df['time'] > $setupTime]
df['engine'] = '$1'; \
df['type'] = '$type'; \
df['name'] = '$name'; \
df_all = df.groupby(['engine', 'type', 'name']).agg(bytes_sent=('bytes_sent', 'sum'), bytes_recv=('bytes_recv', 'sum')).reset_index(); \
df_all['total'] = df_all['bytes_sent'] + df_all['bytes_recv']; \
df_all.to_csv('results/micro_network/network_${1}_${type}_${op_type}_${entries}.csv', index=False); \
df_ip = df.groupby(['engine', 'type', 'name', 'ip']).agg(bytes_sent=('bytes_sent', 'sum'), bytes_recv=('bytes_recv', 'sum')).reset_index(); \
df_ip['total'] = df_ip['bytes_sent'] + df_ip['bytes_recv']; \
df_ip.to_csv('results/micro_network/networkip_${1}_${type}_${op_type}_${entries}.csv', index=False)"

        done <<< ${ARGS[$type]}
    done
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_network

# crdv
if [[ $ENGINES == *"crdv"* ]]; then
    cp $CONFIG backup.yaml
    sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
    run crdv $METRICS_SERVER_CRDV
fi

# riak
if [[ $ENGINES == *"riak"* ]]; then
    cp $CONFIG_RIAK backup.yaml
    run riak $METRICS_SERVER_RIAK
fi

# pg_crdt
if [[ $ENGINES == *"pg_crdt"* ]]; then
    # remote server
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: remote/" backup.yaml
    run pg_crdt_remote $METRICS_SERVER_PG_CRDT

    # local first clients, state-based
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: local/" backup.yaml
    sed -i'' "s/replication:.*/replication: state/" backup.yaml
    run pg_crdt_local_state $METRICS_SERVER_PG_CRDT

    # local first clients, op-based
    cp $CONFIG_PG_CRDT backup.yaml
    sed -i'' "s/mode:.*/mode: local/" backup.yaml
    sed -i'' "s/replication:.*/replication: operation/" backup.yaml
    run pg_crdt_local_op $METRICS_SERVER_PG_CRDT
fi

# delete the config backup
rm backup.yaml

for type in "${TYPES[@]}"; do
    xorder=()
    while IFS= read -r arg; do
        IFS='|' read -r op_type entries name <<< "$arg"
        xorder+=($name)
    done <<< ${ARGS[$type]}

    python3 plot_bar.py results/micro_network/network_*.csv \
        -x name -f "type == '$type'" -y "total.astype('float') / 1e6" -g "engine" \
        -xname "" -yname "Total network transfer (MB)" -height 3 -ncols 3 \
        -xorder "${xorder[@]//\\\\/\\}" -gorder crdv riak pg_crdt_remote pg_crdt_local_state pg_crdt_local_op \
        -colors "#034078" "#D85343" "#C7C3C3" "#8C8888" "#4A4848" -hatches '///' '\\\' '___' '|||' 'xxx' -loc "upper left" \
        -o results/micro_network/network_$type.png
done
