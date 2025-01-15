import argparse
import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import json
import os
import pandas as pd
import seaborn as sns
import dateutil

# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('file', type=str, help='File with JSON log to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name (if none is provided, re_log.png will be used)', action='store', required=False)
parser.add_argument('-height', type=float, help='Plot height', action='store', default=3)
parser.add_argument('-width', type=float, help='Plot width', action='store', default=3)
parser.add_argument('-ymin', type=int, help='Y-axis minimum', action='store')
parser.add_argument('-ymax', type=int, help='Y-axis maximum', action='store')
parser.add_argument('-f', type=int, help='Filter by site id', action='store')
parser.add_argument('-a', type=float, help='Aggregate measurements (seconds)', action='store', default=1)
parser.add_argument('-g', '--group', type=str, help='Group lines by field', action='store', required=False)
parser.add_argument('-loc', type=str, help='Legend location', action='store', default='best')
parser.add_argument('--log', default=False, help='Make the y-axis log-scale', action='store_true')
parser.add_argument('-palette', type=str, help='Color palette', action='store', default='rocket_r')
parser.add_argument('-colors', nargs='+', type=str, help='List of colors to use', action='store')
parser.add_argument('-saturation', type=float, help='Color saturation', default=1)
args = parser.parse_args()

if not os.path.isfile(args.file):
    exit(f'File {args.file} does not exist.')

# read json
data = []
begin = None
currNumWorkers = 0

with open(args.file) as f:
    for line in f:
        entry = json.loads(line)
        t = dateutil.parser.parse(entry["time"])
        if entry["message"] == "Running":
            begin = t
        elif entry["message"] == "Run started":
            currNumWorkers = entry["workers"]
        elif entry["message"] == "delay":
            if begin is not None and args.f is None or entry["connId"] == args.f:
                ts = int((t - begin).total_seconds() / args.a) * args.a
                data.append({'Site': entry["connId"], 'Workers': str(currNumWorkers), 'time': ts, 'delay': entry["delay"]})
 
df = pd.DataFrame(data)

# color
palette = (
    [sns.desaturate(c, args.saturation) for c in args.colors]
    if args.colors is not None
    else sns.color_palette(args.palette, desat=args.saturation)
)

# plot
plt.figure(figsize=(args.width, args.height))
ax = sns.lineplot(data=df, x=df['time'], y=df['delay'], hue=df.eval(args.group), errorbar=None,
                  palette=palette)
ax.grid(color='#eee', linewidth=0.4)

# log scale
if args.log:
    ax.set_yscale('log')

# legend
if ax.legend_ is not None:
    ax.legend_.set_title(None)
plt.legend(loc=args.loc, labelspacing=0.4, ncols=3, columnspacing=0.8, handlelength=1).get_frame().set_linewidth(0)

# labels
ax.set_xlabel("Time (s)")
ax.set_ylabel("Delay (operations to apply)")
ax.set_ylim(0)

# y limit
if args.ymin:
    plt.ylim(bottom=args.ymin)
if args.ymax:
    plt.ylim(top=args.ymax)

plt.tight_layout()

# export
plt.savefig(args.output or f'delay.png', dpi=300)
