#!/bin/bash

# database(s) should already exist (see schema/create_cluster.sh)
CONFIG="conf/nested.yaml"
LEVELS=(1 2 3 4 5 6 7 8 9 10)
TIME=60
WARMUP=3
COOLDOWN=3
RUNS=3
WORKERS=1
STRUCTURES=100000

# backup the config file
cp $CONFIG backup.yaml

# build
go build > /dev/null

# create the required directories
mkdir -p results/nested

# shared configs
sed -i'' "s/time:.*/time: $TIME/" backup.yaml
sed -i'' "s/warmup:.*/warmup: $WARMUP/" backup.yaml
sed -i'' "s/cooldown:.*/cooldown: $COOLDOWN/" backup.yaml
sed -i'' "s/runs:.*/runs: $RUNS/" backup.yaml
sed -i'' "s/workers:.*/workers: [$WORKERS]/" backup.yaml
sed -i'' "s/items:.*/items: $STRUCTURES/" backup.yaml

# run
i=0
for level in "${LEVELS[@]}"; do
    echo "Running level=$level"
    sed -i'' "s/nestingLevel:.*/nestingLevel: $level/" backup.yaml
    views=$(printf 'MapAwLww %.0s' $(seq $level))
    query=$(python3 ../schema/nested_views.py -i $views)
    sed -i'' "s/readQuery:.*/readQuery: $query/" backup.yaml

    ./benchmarks --conf backup.yaml --no-log > out.txt

    # first test, reset the file
    if [ "$i" -eq 0 ]; then
        grep -Po "(?<=Csv:).*" out.txt > results/nested/results.csv
        grep -Po "(?<=CsvOps:).*" out.txt > results/nested/ops.csv
        echo "nestingLevel,type,time,p95" > results/nested/times.csv
    # otherwise, ignore the header and append to the file
    else
        grep -Po "(?<=Csv:).*" out.txt | tail -n +2 >> results/nested/results.csv
        grep -Po "(?<=CsvOps:).*" out.txt | tail -n +2 >> results/nested/ops.csv
    fi

    echo -n "$level,plan," >> results/nested/times.csv
    grep -Po "(?<=planTime: ).*" out.txt | tr -d '\n' >> results/nested/times.csv
    echo -n "," >> results/nested/times.csv
    grep -Po "(?<=planP95: ).*" out.txt >> results/nested/times.csv
    echo -n "$level,exec," >> results/nested/times.csv
    grep -Po "(?<=execTime: ).*" out.txt | tr -d '\n' >> results/nested/times.csv
    echo -n "," >> results/nested/times.csv
    grep -Po "(?<=execP95: ).*" out.txt >> results/nested/times.csv
    echo -n "$level,plan + exec," >> results/nested/times.csv
    grep -Po "(?<=totalTime: ).*" out.txt | tr -d '\n' >> results/nested/times.csv
    echo -n "," >> results/nested/times.csv
    grep -Po "(?<=totalP95: ).*" out.txt >> results/nested/times.csv
    echo -n "$level,real," >> results/nested/times.csv
    grep -Po "(?<=,read,)\d+\.\d+" out.txt | tr -d '\n' >> results/nested/times.csv
    echo -n "," >> results/nested/times.csv
    grep -Po "(?<=rtP95: ).*" out.txt >> results/nested/times.csv
    ((i++))
done

# delete the config backup
rm backup.yaml

# plot
python3 plot_line.py results/nested/times.csv -x nestingLevel -y "time * 1000" -g "type" \
    -height 3 -width 3 -yname "Time (ms)" -xname "Nesting level" \
    -p "p95 * 1000" -plabel "$\it{p95}$" -ploc "upper right" -loc "upper left" -ymax 2 \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -markers 'o' '^' 'X' 's' \
    -gorder plan exec "plan + exec" real -o results/nested/rt.png

python3 plot_line.py results/nested/results.csv -x nestingLevel -y planSize \
    -height 3 -width 3 -yname "Plan size (lines)" -xname "Nesting size" \
    -o results/nested/plan_size.png
