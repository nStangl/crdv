#!/bin/bash


_plot_line() {
    python3 ../../benchmarks/plot_line.py "$@"
}


_plot_bar() {
    python3 ../../benchmarks/plot_bar.py "$@"
}


# runs the timestamp encoding tests
timestamp_encoding() {
    if [ ! -d ../results/timestamp_encoding ]; then
        echo "  timestamp encoding (results not found)"
    else
        echo "  timestamp encoding"
        _plot_line ../results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
            -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'readKey'" -yformatter '%.1f' \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -loc "upper left" -yticksdelta 1.0 \
            -gorder row array json cube -height 2.6 -width 2.6 -nolegend -o ../results/plots/te_readKey.pdf

        _plot_line ../results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
            -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'write'" \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "lower left" -yticksdelta 0.5 \
            -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 4.4 -o ../results/plots/te_write.pdf

        _plot_line ../results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
            -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'currTime'" \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "lower left" \
            -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 1.7 -yticksdelta 0.2 -o ../results/plots/te_currTime.pdf

        _plot_line ../results/timestamp_encoding/results.csv -x simulatedSites -y "size / 1e9" \
            -g schema -t -xname "Simulated sites" -yname "Database size (GB)" \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 1.39 -yticksdelta 0.2 -o ../results/plots/te_size.pdf

        _plot_line ../results/timestamp_encoding/results.csv -x simulatedSites -y "size / 1e9" \
            -g schema -ncols 4 -legendonly -columnspacing 1.5 \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -gorder row array json cube -height 10 -width 10 -o ../results/plots/te_legend.pdf
    fi
}


# runs the materialization strategy tests
materialization_strategy() {
    if [ ! -d ../results/micro_rw ]; then
        echo "  materialization strategy (results not found)"
    else
        echo "  materialization strategy"
        declare -A colors
        colors["sync"]="#034078"
        colors["async"]="#D85343"
        colors["no-mat"]="#C7C3C3"
        declare -A extra
        extra["sync"]=""
        extra["async"]="-yhide"
        extra["no-mat"]="-yhide"
        for type in "sync" "async" "no-mat"; do
            _plot_line ../results/micro_rw/results_*.csv -x "100 - writePercentage" -y "tps / 1000" \
                -ymin 0 -ymax 172 -f "engine == '$type'" -g "itemsPerStructure" -legendtitle Maps \
                -height 2.33 -width 2.33 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' \
                -fillbetween ${colors[$type]} -xticksdelta 25 -saturation 0.9 -xmax 110 -xmin "-8" -yticksdelta 25 \
                -yname "Throughput (×1e3 tx/s)" -xname "Read percentage" ${extra[$type]} -o ../results/plots/materialization_tps_$type.pdf

            _plot_line ../results/micro_rw/results_*.csv -x "100 - writePercentage" -y "rt * 1000" \
                -ymin 0 -ymax 130 -f "engine == '$type'" -g "itemsPerStructure" -legendtitle Maps \
                -height 2.33 -width 2.33 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' \
                -fillbetween ${colors[$type]} -xticksdelta 25 -ybins 7 -saturation 0.9 -xhide -xmax 110 -xmin "-8" \
                -yname "Response time (ms)" -xname "Read percentage" ${extra[$type]} -o ../results/plots/materialization_rt_$type.pdf
        done
    fi
}


# runs the nested structures tests
nested_structures() {
    if [ ! -d ../results/nested ]; then
        echo "  nested structures (results not found)"
    else
        echo "  nested structures"
        _plot_line ../results/nested/times.csv -x nestingLevel -y "time * 1000" -g "type" \
            -height 2.6 -width 2.6 -yname "Response time (ms)" -xname "Nesting size" \
            -p "p95 * 1000" -plabel "$\it{p95}$" -ploc "upper right" -loc "upper left" \
            -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
            -gorder exec plan "plan + exec" real -ymax 1.9 -xticksdelta 1 -yticksdelta 0.2 -o ../results/plots/nested.pdf
    fi
}


# runs the operations tests
operations() {
    if [ ! -d ../results/micro ]; then
        echo "  operations (results not found)"
    else
        echo "  operations"
        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('counter', '')" -f "operation.str.startswith('counter')" \
            -g "engine" -height 2.2 -width 0.11 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
            -xorder Get Inc Dec -gorder crdv-sync crdv-async native pg_crdt riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#D85343" -saturation 0.9 \
            -hatches '//' '++' '' '__' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o ../results/plots/operations_counter.pdf

        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('register', '')" -f "operation.str.startswith('register')" \
            -g "engine" -height 2.2 -width 0.10 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
            -xorder Get Set -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o ../results/plots/operations_register.pdf

        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('set', '')" -f "operation.str.startswith('set')" \
            -g "engine" -height 2.2 -width 0.14 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
            -xorder Get Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o ../results/plots/operations_set.pdf

        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('map', '')" -f "operation.str.startswith('map')" \
            -g "engine" -height 2.2 -width 0.14 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
            -xorder Get Value Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o ../results/plots/operations_map.pdf

        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('list', '')" -f "operation.str.startswith('list')" \
            -g "engine" -height 2.2 -width 0.10 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
            -xorder Get GetAt Add Append Prepend Rmv -gorder crdv-sync crdv-async native pg_crdt \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" -saturation 0.9 \
            -hatches '//' '++' '' '__' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o ../results/plots/operations_list.pdf

        _plot_bar ../results/micro/results_*.csv \
            -y "rt * 1000" -x "operation.str.replace('register', '')" \
            -g "engine" -columnspacing 2 \
            -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -legendonly -ncols 6 -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -hatches '//' '++' '' '__' 'xx' '\\' -o ../results/plots/operations_legend.pdf
    fi
}


# runs the concurrency tests
concurrency() {
    if [ ! -d ../results/micro_concurrency ]; then
        echo "  concurrency (results not found)"
    else
        echo "  concurrency"
        _plot_line ../results/micro_concurrency/results_*.csv \
            -y "tps" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Throughput (tx/s)" -g "engine" \
            -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -markers 'o' '^' 'X' 's' 'P' 'v' -height 2.6 -width 2.6 -xbins 6 -yticksdelta 5000 -kxticks -kyticks \
            -o ../results/plots/concurrency_tps.pdf

        _plot_line ../results/micro_concurrency/results_*.csv \
            -y "rt * 1000" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Response time (ms)" -g "engine" \
            -gorder crdv-sync crdv-async native pg_crdt electric riak \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -markers 'o' '^' 'X' 's' 'P' 'v' -height 2.6 -width 2.6 --log -xbins 6 -kxticks \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower left" -loc "lower left" \
            -o ../results/plots/concurrency_rt.pdf

        _plot_line ../results/micro_concurrency/results_*.csv \
            -y "startSize / 1e6" -x "initialOpsPerStructure" -t -g "engine" -width 10 -height 10 \
            -gorder crdv-sync crdv-async native pg_crdt electric riak -ncols 6 \
            -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -ploc "upper right" \
            -markers 'o' '^' 'X' 's' 'P' 'v' -legendonly -columnspacing 1 -o ../results/plots/concurrency_legend.pdf
    fi
}


# runs the storage structures tests
storage_structures() {
    if [ ! -d ../results/micro_storage ]; then
        echo "  storage (results not found)"
    else
        echo "  storage"
        _plot_line ../results/micro_storage/results_*.csv \
            -y "startSize / 1e6" -x "initialOpsPerStructure" -t \
            -xname "Map size (# entries)" -yname "Storage used (MB)" -g "engine" -height 2.3 -width 2.2 \
            -gorder crdv-sync crdv-async native pg_crdt electric riak -yticksdelta 5 -rx 90 \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -markers 'o' '^' 'X' 's' 'P' 'v' -o ../results/plots/storage.pdf

        _plot_line ../results/micro_storage/ops_*.csv -f "operation == 'mapValue'" \
            -y "rt * 1000" -x "initialOpsPerStructure" -t \
            -g "engine" -height 2.3 -width 1.8 -xname "Map size" -yname "Response time (ms)" \
            -gorder crdv-sync crdv-async native pg_crdt electric riak -ymin 0 -ymax 7.8 -yticksdelta 1 -rx 90 \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" \
            -markers 'o' '^' 'X' 's' 'P' 'v' -o ../results/plots/storage_rt_read.pdf

        _plot_line ../results/micro_storage/ops_*.csv -f "operation == 'mapAdd'" \
            -y "rt * 1000" -x "initialOpsPerStructure" -t \
            -g "engine" -height 2.3 -width 1.8 -xname "Map size" -yname "Response time (ms)" \
            -gorder crdv-sync crdv-async native pg_crdt electric riak -ymin 0 -ymax 7.8 -yticksdelta 1 -rx 90 \
            -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
            -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -yhide \
            -markers 'o' '^' 'X' 's' 'P' 'v' -o ../results/plots/storage_rt_write.pdf

        _plot_line ../results/micro_storage/results_*.csv \
            -y "startSize / 1e6" -x "initialOpsPerStructure" -t -g "engine" -width 10 \
            -gorder crdv-sync crdv-async native pg_crdt electric riak -ncols 6 \
            -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -ploc "upper right" \
            -markers 'o' '^' 'X' 's' 'P' 'v' -legendonly -columnspacing 1 -o ../results/plots/storage_legend.pdf
    fi
}


# run the storage per site tests
storage_sites() {
    if [ ! -d ../results/micro_storage_sites ]; then
        echo "  storage sites (results not found)"
    else
        echo "  storage sites"
        _plot_line ../results/micro_storage_sites/results_*.csv \
            -y "startSize / 1e6" -x "sites" -t \
            -xname "Number of sites" -yname "Storage used (MB)" -g "engine" -height 2.6 -width 2.6 \
            -gorder crdv native electric riak -yticksdelta 10 -ymax 109 \
            -ncols 1 -colors "#034078" "#0A1128" "#DF7E20" "#D85343" -saturation 0.9 \
            -handletextpad 0.6 -handlelength 2 -lfontsize 9 \
            -markers 'o' 'X' 'P' 'v' -o ../results/plots/storage_sites.pdf
    fi
}


# runs the network tests
network() {
    if [ ! -d ../results/micro_network ]; then
        echo "  network (results not found)"
    else
        echo "  network"
        _plot_bar ../results/micro_network/network_*.csv \
            -x name -f "type == 'map'" -y "total.astype('float') / 1e6" -g "engine" -columnspacing 0.6 \
            -xname "" -yname "Network transfer (MB)" -height 2.3 -width 0.097 -ncols 3 -loc "upper left" -ymax 290 \
            -xorder "$\mathregular{Get_1}$" "$\mathregular{Get_{100}}$" "$\mathregular{Value_1}$" \
                    "$\mathregular{Value_{100}}$" "$\mathregular{Add_1}$" "$\mathregular{Add_{100}}$" \
            -gorder crdv riak pg_crdt_remote pg_crdt_local_state pg_crdt_local_op \
            -colors "#034078" "#D85343" "#C7C3C3" "#8C8888" "#4A4848" -saturation 0.9 -hatches '//' '\\' '__' '||' 'xx' \
            -columnspacing 1.3 -handletextpad 0.6 -o ../results/plots/network_map.pdf
    fi
}


# runs the freshness tests
freshness() {
    if [ ! -d ../results/delay ]; then
        echo "  freshness (results not found)"
    else
        echo "  freshness"
        . ../conf/freshness.sh
        declare -A extra
        extra["crdv"]=""
        extra["riak"]="-yhide"
        extra["pg_crdt"]="-yhide"
        for engine in $ENGINES; do
            _plot_line ../results/delay/${engine}_*.csv \
                -x time -y delay -xname "Time (seconds)" -yname "Delay (missing ops)" \
                -g "workers" -height 2.1 -width 2.2 -ymin 1 -ymax 500000 --log ${extra[$engine]} -xticksdelta 15 \
                -yticks 1 10 100 1000 10000 100000 -xmax $(($TIME + $POST_END_WAIT)) \
                -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
                -nolegend -saturation 0.7 -xhide -o ../results/plots/freshness_$engine.pdf

            _plot_line ../results/delay/${engine}_*.csv \
                -x time -y "tps" -ymin 1 -ymax 50000 -xname "Time (seconds)" -yname "Throughput (tx/s)" \
                -g "workers" -height 2.1 -width 2.2 ${extra[$engine]} --log -xticksdelta 15 -xmax $(($TIME + $POST_END_WAIT)) \
                -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
                -nolegend -saturation 0.7 -o ../results/plots/freshness_tps_$engine.pdf
        done

        _plot_line ../results/delay/crdv_*.csv \
            -x time -y "tps / 1000" -width 10 -legendtitle Clients -legendtitleinline \
            -g "workers" -ncols 6 -columnspacing 1.4 -saturation 0.7 -handlelength 1 -handletextpad 0.5 \
            -legendonly -o ../results/plots/freshness_legend.pdf
    fi
}


# runs the multiple sites tests
multiple_sites() {
    if [ ! -d ../results/micro_scale ]; then
        echo "  multiple sites (results not found)"
    else
        echo "  multiple sites"
        _plot_line ../results/micro_scale/*_r.csv -x "sites" -y "tps / 1000" -g "engine"  \
            -f "operation == 'total'" -xname "Sites" -yname "Throughput (×1000 tx/s)" -t \
            -colors "#034078" "#1282A2" "#D85343" -markers "X" "o" "^" \
            -gorder crdv-sync crdv-async riak -nolegend \
            -height 2.6 -width 2.6 -o ../results/plots/sites_r.pdf

        _plot_line ../results/micro_scale/*_w.csv -x "sites" -y "tps / 1000" -g "engine"  \
            -f "operation == 'total'" -xname "Sites" -yname "Throughput (×1000 tx/s)" -t \
            -colors "#034078" "#1282A2" "#D85343" "#034078" "#1282A2" "#D85343" -markers "X" "o" "^" "X" "o" "^" \
            -gorder crdv-sync crdv-async riak crdv-sync-ring crdv-async-ring riak-ring \
            -dashes '(10000, 1)' '(10000, 1)' '(10000, 1)' '(1, 1)' '(1, 1)' '(1, 1)' -nolegend \
            -height 2.6 -width 2.6 -o ../results/plots/sites_w.pdf

        _plot_line ../results/micro_scale/*_w.csv -x "sites" -y "tps" -g "engine"  \
            -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t \
            -colors "#034078" "#1282A2" "#D85343" "#222222" -markers "X" "o" "^" "." \
            -dashes '(10000, 1)' '(10000, 1)' '(10000, 1)' '(1, 1)' \
            -gorder crdv-sync crdv-async riak "\$ring\$" -legendonly -ncols 4 -columnspacing 1 -handletextpad 0.4 \
            -height 100 -width 100 -o ../results/plots/sites_legend.pdf
    fi
}


# crop plots
crop() {
    for plot in ../results/plots/*.pdf; do
        [ -e "$plot" ] || continue
        pdfcrop $plot $plot > /dev/null &
    done
    wait
}


main() {
    echo "Plotting"
    mkdir -p ../results/plots
    timestamp_encoding &
    materialization_strategy &
    nested_structures &
    operations &
    concurrency &
    storage_structures &
    storage_sites &
    network &
    freshness &
    multiple_sites &
    wait
    crop
}


main "$@"
