#!/bin/bash

# database should already exist (see schema/create_cluster.sh)
CONFIG="conf/timestampEncoding.yaml"
SCHEMAS=(row array json cube)
SITES=(1 2 4 6 8 10 12 14 16)
TIME=60
WARMUP=3
COOLDOWN=3
RUNS=3
WORKERS=1
ITEMS=1000
OPS=1000

# backup the config file
cp $CONFIG backup.yaml

# build
go build > /dev/null

# create the required directories
mkdir -p results/timestamp_encoding

# shared configs
sed -i'' "s/time:.*/time: $TIME/" backup.yaml
sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
sed -i'' "s/items:.*/items: $ITEMS/" backup.yaml
sed -i'' "s/ops:.*/ops: $OPS/" backup.yaml

i=0
for schema in "${SCHEMAS[@]}"; do
    for sites in "${SITES[@]}"; do
        echo "Running $schema $sites"
        sed -i'' "s/schema:.*/schema: $schema/" backup.yaml
        sed -i'' "s/sites:.*/sites: $sites/" backup.yaml
        ./benchmarks --conf backup.yaml --no-log > out.txt

        # first test, reset the file
        if [ "$i" -eq 0 ]; then
            grep -Po "(?<=Csv:).*" out.txt > results/timestamp_encoding/results.csv
            grep -Po "(?<=CsvOps:).*" out.txt > results/timestamp_encoding/results_ops.csv
        # otherwise, ignore the header and append to the file
        else
            grep -Po "(?<=Csv:).*" out.txt | tail -n +2 >> results/timestamp_encoding/results.csv
            grep -Po "(?<=CsvOps:).*" out.txt | tail -n +2 >> results/timestamp_encoding/results_ops.csv
        fi

        ((i++))
    done
done

# delete the config backup
rm backup.yaml

# plot
python3 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Sites" -yname "Response time (ms)" -f "operation == 'readKey'" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "center left" -loc "upper left" \
    -gorder row array json cube -height 3 -width 3 -o results/timestamp_encoding/readKey.png

python3 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Sites" -yname "Response time (ms)" -f "operation == 'write'" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "lower left" \
    -gorder row array json cube -height 3 -width 3 -o results/timestamp_encoding/write.png

python3 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Sites" -yname "Response time (ms)" -f "operation == 'currTime'" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "upper left" \
    -gorder row array json cube -height 3 -width 3 -o results/timestamp_encoding/currTime.png

python3 plot_line.py results/timestamp_encoding/results.csv -x simulatedSites -y "size / 1e9" \
    -g schema -t -xname "Sites" -yname "Database size (GB)" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -markers 'o' '^' 'X' 's' \
    -gorder row array json cube -height 3 -width 3 -o results/timestamp_encoding/size.png
