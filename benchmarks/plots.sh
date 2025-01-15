mkdir -p plots


# timestamp encoding

python3.12 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'readKey'" -yformatter '%.1f' \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -loc "upper left" -yticksdelta 1.0 \
    -gorder row array json cube -height 2.6 -width 2.6 -nolegend -o plots/te_readKey.pdf

python3.12 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'write'" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "lower left" -yticksdelta 0.5 \
    -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 4.4 -o plots/te_write.pdf

python3.12 plot_line.py results/timestamp_encoding/results_ops.csv -x simulatedSites -y "rt*1000" \
    -g schema -t -xname "Simulated sites" -yname "Response time (ms)" -f "operation == 'currTime'" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower right" -loc "lower left" \
    -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 1.7 -yticksdelta 0.2 -o plots/te_currTime.pdf

python3.12 plot_line.py results/timestamp_encoding/results.csv -x simulatedSites -y "size / 1e9" \
    -g schema -t -xname "Simulated sites" -yname "Database size (GB)" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -gorder row array json cube -height 2.6 -width 2.6 -nolegend -ymax 1.39 -yticksdelta 0.2 -o plots/te_size.pdf

pdfcrop plots/te_readKey.pdf plots/te_readKey.pdf
pdfcrop plots/te_write.pdf plots/te_write.pdf
pdfcrop plots/te_currTime.pdf plots/te_currTime.pdf
pdfcrop plots/te_size.pdf plots/te_size.pdf


python3.12 plot_line.py results/timestamp_encoding/results.csv -x simulatedSites -y "size / 1e9" \
    -g schema -ncols 4 -legendonly -ymax 0 -columnspacing 1.5 \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -gorder row array json cube -height 10 -width 10 -o plots/te_legend.pdf

pdfcrop plots/te_legend.pdf plots/te_legend.pdf
#pdfcrop -margins "-3 -3 -3 -3" plots/te_legend.pdf plots/te_legend_no_border.pdf



# operation

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('counter', '')" -f "operation.str.startswith('counter')" \
    -g "engine" -height 2.2 -width 0.11 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Inc Dec -gorder crdv-sync crdv-async native pg_crdt riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_counter.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('register', '')" -f "operation.str.startswith('register')" \
    -g "engine" -height 2.2 -width 0.10 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Set -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_register.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('set', '')" -f "operation.str.startswith('set')" \
    -g "engine" -height 2.2 -width 0.14 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_set.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('map', '')" -f "operation.str.startswith('map')" \
    -g "engine" -height 2.2 -width 0.14 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Value Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_map.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('list', '')" -f "operation.str.startswith('list')" \
    -g "engine" -height 2.2 -width 0.10 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get GetAt Add Append Prepend Rmv -gorder crdv-sync crdv-async native pg_crdt \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_list.pdf

pdfcrop plots/op_counter.pdf plots/op_counter.pdf
pdfcrop plots/op_register.pdf plots/op_register.pdf
pdfcrop plots/op_set.pdf plots/op_set.pdf
pdfcrop plots/op_map.pdf plots/op_map.pdf
pdfcrop plots/op_list.pdf plots/op_list.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('register', '')" \
    -g "engine" -columnspacing 2 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -legendonly -ncols 6 -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -o plots/op_legend.pdf

pdfcrop plots/op_legend.pdf plots/op_legend.pdf


# operations compact

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('counter', '')" -f "operation.str.startswith('counter')" \
    -g "engine" -height 2.2 -width 0.09 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Inc Dec -gorder crdv-sync crdv-async native pg_crdt riak -yhide \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_counter_compact.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('register', '')" -f "operation.str.startswith('register')" \
    -g "engine" -height 2.2 -width 0.08 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Set -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_register_compact.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('set', '')" -f "operation.str.startswith('set')" \
    -g "engine" -height 2.2 -width 0.12 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak -yhide \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_set_compact.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('map', '')" -f "operation.str.startswith('map')" \
    -g "engine" -height 2.08 -width 0.12 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get Value Contains Add Rmv -gorder crdv-sync crdv-async native pg_crdt electric riak -yhide \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' 'xx' '\\' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_map_compact.pdf

python3.12 plot_bar.py results/micro/results_*.csv \
    -y "rt * 1000" -x "operation.str.replace('list', '')" -f "operation.str.startswith('list')" \
    -g "engine" -height 2.3 -width 0.08 -xname "" -yname "Response time (ms)" -ymax 6.8 -yticksdelta 1 \
    -xorder Get GetAt Add Append Prepend Rmv -gorder crdv-sync crdv-async native pg_crdt \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -hatches '//' '++' '' '__' -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -o plots/op_list_compact.pdf

pdfcrop plots/op_counter_compact.pdf plots/op_counter_compact.pdf
pdfcrop plots/op_register_compact.pdf plots/op_register_compact.pdf
pdfcrop plots/op_set_compact.pdf plots/op_set_compact.pdf
pdfcrop plots/op_map_compact.pdf plots/op_map_compact.pdf
pdfcrop plots/op_list_compact.pdf plots/op_list_compact.pdf


# log rt

#python3.12 plot_log_rt.py results/micro_log_rt/log_*.log -f "operation == 'registerGet'" \
#    -gorder crdv-sync crdv-async native pg_crdt electric riak -b 1 -c 1 -height 2.6 -width 2.6 \
#    -ymin 0 -ymax 10.5 -g type -loc "upper left" --no-title \
#    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' 'P' 'v' \
#    -markevery 10 -o plots/op_log_read.pdf
#
#python3.12 plot_log_rt.py results/micro_log_rt/log_*.log -f "operation == 'registerSet'" \
#    -gorder crdv-sync crdv-async native pg_crdt electric riak -b 1 -c 1 -height 2.6 -width 2.6 \
#    -ymin 0 -ymax 21 -g type -loc "upper left" --no-title \
#    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' 'P' 'v' \
#    -markevery 10 -o plots/op_log_write.pdf
#
#pdfcrop plots/op_log_read.pdf plots/op_log_read.pdf
#pdfcrop plots/op_log_write.pdf plots/op_log_write.pdf



#python3.12 plot_line.py results/micro_size/results_sync.csv -x "initialOpsPerStructure" -y "rt * 1000" \
#    -g "operation.str.replace('map', '')" -f "operation.str.startswith('map')" -height 2.6 -width 2.6 \
#    -gorder Get Value Contains Add Rmv -xname "Map size" -yname "Response time (ms)" \
#    -colors "#034078" "#1282A2" "#0A1128" "#DF7E20" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' 'P' \
#    --log --xlog -ymin 0.101 -ymax 2100 -o plots/size_sync.pdf
#
#python3.12 plot_line.py results/micro_size/results_async.csv -x "initialOpsPerStructure" -y "rt * 1000" \
#    -g "operation.str.replace('map', '')" -f "operation.str.startswith('map')" -height 2.6 -width 2.6 \
#    -gorder Get Value Contains Add Rmv -xname "Map size" -yname "Response time (ms)" \
#    -colors "#034078" "#1282A2" "#0A1128" "#DF7E20" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' 'P' \
#    --log --xlog -ymin 0.101 -ymax 2100 -o plots/size_async.pdf
#
#pdfcrop plots/size_sync.pdf plots/size_sync.pdf
#pdfcrop plots/size_async.pdf plots/size_async.pdf 



# nested


python3.12 plot_line.py results/nested/times.csv -x nestingLevel -y "time * 1000" -g "type" \
    -height 2.6 -width 2.6 -yname "Response time (ms)" -xname "Nesting size" \
    -p "p95 * 1000" -plabel "$\it{p95}$" -ploc "upper right" -loc "upper left" \
    -colors "#1282A2" "#034078" "#0A1128" "#D85343" -saturation 0.9 -markers 'o' '^' 'X' 's' \
    -gorder exec plan "plan + exec" real -ymax 1.9 -xticksdelta 1 -yticksdelta 0.2 -o plots/nested.pdf

pdfcrop plots/nested.pdf plots/nested.pdf



# rw

# agg with types separated
declare -A colors
colors["sync"]="#034078"
colors["async"]="#D85343"
colors["no-mat"]="#C7C3C3"
declare -A extra
extra["sync"]=""
extra["async"]="-yhide"
extra["no-mat"]="-yhide"
for type in "sync" "async" "no-mat"; do
    python3.12 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "tps / 1000" \
        -ymin 0 -ymax 172 -f "engine == '$type'" -g "itemsPerStructure" -legendtitle Maps \
        -height 2.33 -width 2.33 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' \
        -fillbetween ${colors[$type]} -xticksdelta 25 -saturation 0.9 -xmax 110 -xmin "-8" -yticksdelta 25 \
        -yname "Throughput (×1e3 tx/s)" -xname "Read percentage" ${extra[$type]} -o plots/rw_tps_$type.pdf
    pdfcrop plots/rw_tps_$type.pdf plots/rw_tps_$type.pdf

    python3.12 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "rt * 1000" \
        -ymin 0 -ymax 130 -f "engine == '$type'" -g "itemsPerStructure" -legendtitle Maps \
        -height 2.33 -width 2.33 -colors ${colors[$type]} ${colors[$type]} -dashes '(10000, 1)' '(2, 2)' \
        -fillbetween ${colors[$type]} -xticksdelta 25 -ybins 7 -saturation 0.9 -xhide -xmax 110 -xmin "-8" \
        -yname "Response time (ms)" -xname "Read percentage" ${extra[$type]} -o plots/rw_rt_$type.pdf
    pdfcrop plots/rw_rt_$type.pdf plots/rw_rt_$type.pdf
done


#python3.12 plot_line.py results/micro_rw/results_*.csv -x "100 - writePercentage" -y "rt * 1000" \
#    -ymax 0 -f "engine == '$type'" -g "itemsPerStructure" -legendtitle Maps \
#    -colors "black" -saturation 0.9 -dashes '(10000, 1)' '(2, 2)' \
#    -legendonly -ncols 2 -columnspacing 2 -o plots/rw_legend.pdf





# old scale

#python3.12 plot_line.py results/micro_scale/*_rw.csv -x "sites" -y "tps / 1000" -g "engine"  \
#    -f "operation == 'total'" -xname "Number of sites" -yname "Throughput (×1000 tx/s)" -t \
#    -colors "#034078" "#1282A2" "#D85343" -saturation 0.9 -markers "X" "o" "^"  \
#    -gorder sync async riak -loc "lower right" \
#    -height 2.6 -width 2.6 -handletextpad 0.3 -o plots/scale_tps.pdf
#
#python3.12 plot_line.py results/micro_scale/*_rw.csv -x "sites" -y "rt * 1000" \
#    -extracol " " -g "engine + _extracol + operation.str.replace('read', 'r').replace('write', 'w')"  \
#    -f "operation != 'total'" -xname "Number of sites" -yname "Response time (ms)" -t \
#    -colors "#034078" "#034078" "#1282A2" "#1282A2" "#D85343" "#D85343" -saturation 0.9 -markers "X" "o" "^" "P" "s" "v" \
#    -dashes '(10000, 1)' '(2, 2)' '(10000, 1)' '(2, 2)' '(10000, 1)' '(2, 2)' \
#    -gorder "sync r" "sync w" "async r" "async w" "riak r" "riak w" -loc "upper center" \
#    -sorder r w -height 2.6 -width 2.6 -ymax 18 -ncols 2 -lfontsize 9 -handletextpad 0.3 -o plots/scale_rt.pdf
#
#pdfcrop plots/scale_tps.pdf plots/scale_tps.pdf
#pdfcrop plots/scale_rt.pdf plots/scale_rt.pdf


# scale

python3.12 plot_line.py results/micro_scale/*_r.csv -x "sites" -y "tps" -g "engine"  \
    -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t -ky \
    -colors "#034078" "#1282A2" "#D85343" -markers "X" "o" "^"  \
    -gorder crdv-sync crdv-async riak -nolegend -yticksdelta 100000 -ymin 0 -ymax 850000 \
    -height 2.6 -width 2.6 -o plots/scale_r.pdf

#python3.12 plot_line.py results/micro_scale/*_w.csv -x "sites" -y "tps" -g "engine"  \
#    -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t -ky \
#    -colors "#034078" "#1282A2" "#D85343" -markers "X" "o" "^" \
#    -gorder crdv-sync crdv-async riak -nolegend -yticksdelta 20000 -ymax 190000 -ymin 0 \
#    -height 2.6 -width 2.1 -o plots/scale_w.pdf

#python3.12 plot_line.py results/micro_scale/*_w_ring.csv -x "sites" -y "tps" -g "engine"  \
#    -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t -ky \
#    -colors "#034078" "#1282A2" "#D85343" -markers "X" "o" "^" \
#    -gorder crdv-sync crdv-async riak -nolegend -yticksdelta 20000 -ymax 190000 -ymin 0 -yhide \
#    -height 2.6 -width 2.1 -o plots/scale_w_ring.pdf


python3.12 plot_line.py results/micro_scale/*_w*.csv -x "sites" -y "tps" -g "engine + _file.str[-1]"  \
    -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t -ky \
    -gorder crdv-syncw crdv-asyncw riakw crdv-syncg crdv-asyncg riakg \
    -colors "#034078" "#1282A2" "#D85343" "#034078" "#1282A2" "#D85343" -markers "X" "o" "^" "X" "o" "^" \
    -dashes '(10000, 1)' '(10000, 1)' '(10000, 1)' '(1, 1)' '(1, 1)' '(1, 1)' \
    -nolegend -yticksdelta 20000 -ymax 190000 -ymin 0 \
    -height 2.6 -width 2.6 -o plots/scale_w_both.pdf

pdfcrop plots/scale_r.pdf plots/scale_r.pdf
pdfcrop plots/scale_w.pdf plots/scale_w.pdf
pdfcrop plots/scale_w_ring.pdf plots/scale_w_ring.pdf
pdfcrop plots/scale_w_both.pdf plots/scale_w_both.pdf


python3.12 plot_line.py results/micro_scale/*_w.csv -x "sites" -y "tps" -g "engine"  \
    -f "operation == 'total'" -xname "Sites" -yname "Throughput (tx/s)" -t \
    -colors "#034078" "#1282A2" "#D85343" "#222222" -markers "X" "o" "^" "." -dashes '(10000, 1)' '(10000, 1)' '(10000, 1)' '(1, 1)' \
    -gorder crdv-sync crdv-async riak "\$ring\$" -legendonly -ncols 4 -columnspacing 1 -handletextpad 0.4 \
    -height 100 -width 100 -o plots/scale_legend.pdf

pdfcrop plots/scale_legend.pdf plots/scale_legend.pdf



# old delay


#python3.12 plot_log_delay.py results/delay/log_1s.log -g 'Workers' -f 0 --log -a 0.5 -ymax 4000000 \
#    -loc "upper center" -width 2.6 -height 2.6 -palette "rocket_r" -saturation 0.8 -o plots/delay_1s.pdf
#
#python3.12 plot_log_delay.py results/delay/log_100ms.log -g 'Workers' -f 0 --log -a 0.5 -ymax 150000  \
#    -loc "upper center" -width 2.6 -height 2.6 -palette "rocket_r" -saturation 0.8 -o plots/delay_100ms.pdf
#
#pdfcrop plots/delay_1s.pdf plots/delay_1s.pdf
#pdfcrop plots/delay_100ms.pdf plots/delay_100ms.pdf




# delay

ENGINES="crdv pg_crdt riak"
declare -A extra
extra["crdv"]=""
extra["riak"]="-yhide"
extra["pg_crdt"]="-yhide"

TIME=60
PARTITION_NETWORK_START=25 # seconds into the run
PARTITION_NETWORK_DURATION=10 # seconds
for engine in $ENGINES; do
    python3.12 plot_line.py results/delay/${engine}_*.csv \
        -x time -y delay -xname "Time (seconds)" -yname "Delay (missing ops)" \
        -g "workers" -height 2.1 -width 2.2 -ymin 1 -ymax 500000 --log ${extra[$engine]} -xticksdelta 15 \
        -yticks 1 10 100 1000 10000 100000 \
        -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
        -nolegend -saturation 0.7 -xhide -o plots/delay_$engine.pdf
    pdfcrop plots/delay_$engine.pdf plots/delay_$engine.pdf

    python3.12 plot_line.py results/delay/${engine}_*.csv \
        -x time -y "tps" -ymin 1 -ymax 50000 -xname "Time (seconds)" -yname "Throughput (tx/s)" \
        -g "workers" -height 2.1 -width 2.2 ${extra[$engine]} --log -xticksdelta 15 -yticks 1 10 100 1000 10000 \
        -rvlines $PARTITION_NETWORK_START $(($PARTITION_NETWORK_START + $PARTITION_NETWORK_DURATION)) -bvlines $TIME \
        -nolegend -saturation 0.7 -o plots/delay_tps_$engine.pdf
    pdfcrop plots/delay_tps_$engine.pdf plots/delay_tps_$engine.pdf
done

python3.12 plot_line.py results/delay/crdv_*.csv \
    -x time -y "tps / 1000" -ymax 0 -width 10 -legendtitle Clients -legendtitleinline \
    -g "workers" -ncols 6 -columnspacing 1.4 -saturation 0.7 -handlelength 1 -handletextpad 0.5 \
    -legendonly -o plots/delay_legend.pdf

pdfcrop plots/delay_legend.pdf plots/delay_legend.pdf


# storage

python3.12 plot_line.py results/micro_storage/results_*.csv \
    -y "startSize / 1e6" -x "initialOpsPerStructure" -t \
    -xname "Map size (# entries)" -yname "Storage used (MB)" -g "engine" -height 2.3 -width 2.2 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -yticksdelta 5 -rx 90 \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o plots/storage.pdf

python3.12 plot_line.py results/micro_storage/ops_*.csv -f "operation == 'mapValue'" \
    -y "rt * 1000" -x "initialOpsPerStructure" -t \
    -g "engine" -height 2.3 -width 1.8 -xname "Map size" -yname "Response time (ms)" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -ymin 0 -ymax 7.8 -yticksdelta 1 -rx 90 \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o plots/storage_rt_read.pdf

python3.12 plot_line.py results/micro_storage/ops_*.csv -f "operation == 'mapAdd'" \
    -y "rt * 1000" -x "initialOpsPerStructure" -t \
    -g "engine" -height 2.3 -width 1.8 -xname "Map size" -yname "Response time (ms)" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -ymin 0 -ymax 7.8 -yticksdelta 1 -rx 90 \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "upper left" -yhide \
    -markers 'o' '^' 'X' 's' 'P' 'v' -o plots/storage_rt_write.pdf

pdfcrop plots/storage.pdf plots/storage.pdf
pdfcrop plots/storage_rt_read.pdf plots/storage_rt_read.pdf
pdfcrop plots/storage_rt_write.pdf plots/storage_rt_write.pdf


python3.12 plot_line.py results/micro_storage_sites/results_*.csv \
    -y "startSize / 1e6" -x "sites" -t \
    -xname "Number of sites" -yname "Storage used (MB)" -g "engine" -height 2.6 -width 2.6 \
    -gorder crdv native electric riak -yticksdelta 10 -ymax 109 \
    -ncols 1 -colors "#034078" "#0A1128" "#DF7E20" "#D85343" -saturation 0.9 \
    -handletextpad 0.6 -handlelength 2 -lfontsize 9 \
    -markers 'o' 'X' 'P' 'v' -o plots/storage_sites.pdf

pdfcrop plots/storage_sites.pdf plots/storage_sites.pdf


# legend for storage, concurrency
python3.12 plot_line.py results/micro_storage/results_*.csv \
    -y "startSize / 1e6" -x "initialOpsPerStructure" -t -g "engine" -ymax 0 -width 10 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -ncols 3 \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -ploc "upper right" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -legendonly -columnspacing 1 -o plots/full_legend_line.pdf

python3.12 plot_line.py results/micro_storage/results_*.csv \
    -y "startSize / 1e6" -x "initialOpsPerStructure" -t -g "engine" -ymax 0 -width 10 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -ncols 6 \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -ploc "upper right" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -legendonly -columnspacing 1 -o plots/full_legend_line_single_row.pdf

python3.12 plot_line.py results/micro_storage/results_*.csv \
    -y "startSize / 1e6" -x "initialOpsPerStructure" -t -g "engine" -ymax 0 -width 10 \
    -gorder crdv-sync crdv-async native pg_crdt electric riak -ncols 6 \
    -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 -ploc "upper right" \
    -markers 'o' '^' 'X' 's' 'P' 'v' -legendonly -columnspacing 0.8 -handletextpad 0.4 -handlelength 1.7 \
    -o plots/full_legend_line_single_row_compact.pdf


#pdfcrop -margins "-3 -3 -3 -3" plots/full_legend.pdf plots/full_legend_no_border.pdf




# concurrency

python3.12 plot_line.py results/micro_concurrency/results_*.csv \
    -y "tps" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Throughput (tx/s)" -g "engine" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -markers 'o' '^' 'X' 's' 'P' 'v' -height 2.6 -width 2.6 -xbins 6 -yticksdelta 5000 -kxticks -kyticks -ymin 0 -ymax 36000 \
    -o plots/concurrency_tps.pdf

python3.12 plot_line.py results/micro_concurrency/results_*.csv \
    -y "rt * 1000" -x "initialOpsPerStructure" -t -xname "Map size (# entries)" -yname "Response time (ms)" -g "engine" \
    -gorder crdv-sync crdv-async native pg_crdt electric riak \
    -nolegend -colors "#034078" "#1282A2" "#0A1128" "#C7C3C3" "#DF7E20" "#D85343" -saturation 0.9 \
    -markers 'o' '^' 'X' 's' 'P' 'v' -height 2.6 -width 2.6 --log -xbins 6 -kxticks \
    -p "rtP95 * 1000" -plabel "$\it{p95}$" -ploc "lower left" -loc "lower left" \
    -o plots/concurrency_rt.pdf

pdfcrop plots/concurrency_tps.pdf plots/concurrency_tps.pdf
pdfcrop plots/concurrency_rt.pdf plots/concurrency_rt.pdf



# network

ARGS[map]="\
mapGet|1|$\\\mathregular{Get_1}$
mapGet|100|$\\\mathregular{Get_{100}}$
mapValue|1|$\\\mathregular{Value_1}$
mapValue|100|$\\\mathregular{Value_{100}}$
mapAdd|1|$\\\mathregular{Add_1}$
mapAdd|100|$\\\mathregular{Add_{100}}$"

xorder=()
while IFS= read -r arg; do
    IFS='|' read -r op_type ops name <<< "$arg"
    xorder+=($name)
done <<< ${ARGS[map]}

python3.12 plot_bar.py results/micro_network/network_*.csv \
    -x name -f "type == 'map'" -y "total.astype('float') / 1e6" -g "engine" -columnspacing 0.6 \
    -xname "" -yname "Network transfer (MB)" -height 2.3 -width 0.097 -ncols 3 -loc "upper left" -ymax 290 \
    -xorder "${xorder[@]//\\\\/\\}" -gorder crdv riak pg_crdt_remote pg_crdt_local_state pg_crdt_local_op \
    -colors "#034078" "#D85343" "#C7C3C3" "#8C8888" "#4A4848" -saturation 0.9 -hatches '//' '\\' '__' '||' 'xx' \
    -columnspacing 1.3 -handletextpad 0.6 -o plots/network_map.pdf

pdfcrop plots/network_map.pdf plots/network_map.pdf



# remove file names from the metadata

for file in plots/*.pdf; do
    perl -pe 's|(/PTEX.FileName \()([^\)]+)|$1 . " " x length($2)|ge' $file > $file.new
done

rm plots/*.pdf
rename -v "s/.pdf.new/.pdf/" plots/*.pdf.new
