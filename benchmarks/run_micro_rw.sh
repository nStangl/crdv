#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh)
CONFIG="conf/micro_crdv.yaml"
STRUCTURES=(1 1024)
WRITE_PERCENTAGES=(100 95 90 80 70 60 50 40 30 20 10 5 0)
WORKERS=64
TIME=60
WARMUP=3
COOLDOWN=3
RUNS=3

updateConfig() {
    sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
    sed -i'' "s/typesToPopulate:.*/typesToPopulate: [map]/" backup.yaml
    sed -i'' "s/time:.*/time: $TIME/" backup.yaml
    sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
    sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
    sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
    sed -i'' "s/initialOpsPerStructure:.*/initialOpsPerStructure: 1/" backup.yaml
    sed -i'' 's/discardUnmergedWhenFinished:.*/discardUnmergedWhenFinished: true/' backup.yaml
    sed -i'' 's/mergeDelta:.*/mergeDelta: 0.05/' backup.yaml
    sed -i'' 's/mergeBatchSize:.*/mergeBatchSize: 10000/' backup.yaml
}

run() {
    updateConfig

    for structures in "${STRUCTURES[@]}"; do
        for write_percentage in "${WRITE_PERCENTAGES[@]}"; do
            echo "Running $1 $structures $write_percentage"
            read_percentage=$((100 - write_percentage))

            # structures
            sed -i'' "s/itemsPerStructure:.*/itemsPerStructure: $structures/" backup.yaml

            # weights
            sed -i'' 's/weight:.*/weight: 0/' backup.yaml
            awk 'replace {sub("weight: 0", "weight: '$write_percentage'", $0)} {print} {replace=/mapAdd/}' backup.yaml > backup.yaml.tmp
            mv backup.yaml.tmp backup.yaml
            awk 'replace {sub("weight: 0", "weight: '$read_percentage'", $0)} {print} {replace=/mapValue/}' backup.yaml > backup.yaml.tmp
            mv backup.yaml.tmp backup.yaml

            ./benchmarks --conf backup.yaml --no-log > out.txt

            grep -Po "(?<=Csv:).*" out.txt > out.csv
            sed -i'' '1,1{s/$/,writePercentage/}' out.csv
            sed -i'' '2,$s/$/,'$write_percentage'/' out.csv

            cat out.csv > results/micro_rw/results_${1}_${structures}_${write_percentage}.csv

            # remove crdv-
            sed -i'' "s/crdv-//" results/micro_rw/results_${1}_${structures}_${write_percentage}.csv
        done
    done
}

# build
go build > /dev/null

# create the required directories
mkdir -p results/micro_rw

# run
cp $CONFIG backup.yaml
sed -i'' "s/modes:.*/modes: {readMode: local, writeMode: sync}/" backup.yaml
run sync
sed -i'' "s/modes:.*/modes: {readMode: all, writeMode: async}/" backup.yaml
run async
sed -i'' 's/mergeParallelism:.*/mergeParallelism: 0/' backup.yaml
run no-mat
find results/micro_rw/ -type f -name 'results_no-mat_*.csv' -exec sed -i 's/async/no-mat/g' {} \;

# delete the config backup
rm backup.yaml

# agg
python3 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "tps / 1000" \
    -g "engine" -gorder "sync" "async" "no-mat" \
    -height 3 -width 3 -colors "#034078" "#D85343" "#C7C3C3" \
    -saturation 0.9 -markers 'o' '^' 'X' -xticksdelta 25 -reversezorder \
    -yname "Throughput (×1000 tx/s)" -xname "Read percentage" -o results/micro_rw/concurrency_tps.png

python3 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "rt * 1000" \
    -g "engine" -gorder "sync" "async" "no-mat" \
    -height 3 -width 3 -colors "#034078" "#D85343" "#C7C3C3" \
    -saturation 0.9 -markers 'o' '^' 'X' -xticksdelta 25 -reversezorder \
    -yname "Response time (ms)" -xname "Read percentage" -o results/micro_rw/concurrency_rt.png

# agg with types separated
declare -A colors
colors["sync"]="#034078"
colors["async"]="#D85343"
colors["no-mat"]="#C7C3C3"
for type in "sync" "async" "no-mat"; do
    python3 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "tps / 1000" \
        -f "engine == '$type'" -g "itemsPerStructure" -fillbetween ${colors[$type]} \
        -height 3 -width 3 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' -saturation 0.9 \
        -yname "Throughput (×1000 tx/s)" -xname "Read percentage" -o results/micro_rw/concurrency_tps_$type.png

    python3 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "rt * 1000" \
        -f "engine == '$type'" -g "itemsPerStructure" -fillbetween ${colors[$type]} \
        -height 3 -width 3 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' -saturation 0.9 \
        -yname "Response time (ms)" -xname "Read percentage" -o results/micro_rw/concurrency_rt_$type.png
done
