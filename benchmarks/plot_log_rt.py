import argparse
import dateutil.parser
import math
import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import json
import os
import pandas as pd
import seaborn as sns

# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('files', nargs='+', type=str, help='Files with JSON log to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name (if none is provided, re_log.png will be used)', action='store', required=False)
parser.add_argument('-b', type=float, help='Bucket size when computing the rolling average (seconds)', action='store', default=5)
parser.add_argument('-height', type=float, help='Plot height', action='store')
parser.add_argument('-width', type=float, help='Plot width', action='store')
parser.add_argument('-xmin', type=float, help='X-axis minimum', action='store')
parser.add_argument('-xmax', type=float, help='X-axis maximum', action='store')
parser.add_argument('-ymin', type=float, help='Y-axis minimum', action='store')
parser.add_argument('-ymax', type=float, help='Y-axis maximum', action='store')
parser.add_argument('-f', '--filter', type=str, help='Filter lines which match some criteria', action='store', required=False)
parser.add_argument('-g', '--group', type=str, help='Aggregate by some field', action='store')
parser.add_argument('-gorder', nargs='+', type=str, help='Order of the groups', action='store', required=False)
parser.add_argument('-c', type=int, help='Number of columns of subplots', action='store', default=4)
parser.add_argument('-loc', type=str, help='Legend location', action='store', default='best')
parser.add_argument('--no-title', help='Remove the title', action='store_true')
parser.add_argument('-palette', type=str, help='Color palette', action='store', default='rocket_r')
parser.add_argument('-colors', nargs='+', type=str, help='List of colors to use', action='store')
parser.add_argument('-saturation', type=float, help='Color saturation', default=1)
parser.add_argument('-markers', nargs='+', type=str, help='List of markers to use', action='store')
parser.add_argument('-markevery', type=int, help='Delta between each marker', action='store', default=1)
args = parser.parse_args()

validFiles = []
for file in args.files:
    if not os.path.isfile(file):
        print(f'Warning: File {file} does not exist, ignoring it.')
    else:
        validFiles.append(file)

if len(validFiles) == 0:
    exit(f'Error: No valid file to read.')

# read json
data = []
begin = None

for file in validFiles:
    with open(file) as f:
        for line in f:
            entry = json.loads(line)
            if entry["message"] == "Running" and begin is None:
                t = dateutil.parser.parse(entry["time"])
                begin = t
            elif "operation" in entry:
                t = dateutil.parser.parse(entry["real_time"])
                op = entry["operation"]
                type = entry["type"] if 'type' in entry else ''
                x = (t - begin).total_seconds()
                y = entry["rt"] * 1000
                data.append({'operation': op, 'type': type, 'x': x, 'y': y})

df = pd.DataFrame(data)
if args.filter:
    df = df[df.eval(args.filter)]

nCharts = len(df['operation'].unique())
fig, axes = plt.subplots(ncols=args.c, nrows=math.ceil(nCharts/args.c), squeeze=False)
if not args.width:
    args.width = 2.5 * args.c
if not args.height:
    args.height = 2.5 * math.ceil(nCharts/args.c)
fig.set_figwidth(args.width)
fig.set_figheight(args.height)

# color
palette = (
    [sns.desaturate(c, args.saturation) for c in args.colors]
    if args.colors is not None
    else sns.color_palette(args.palette, desat=args.saturation)
)

# plot each subplot
i = 0
j = 0
for operation in sorted(df['operation'].unique()):
    filteredDf = df[df['operation'] == operation]

    if not args.group:
        mean = df.groupby(filteredDf['x'] // args.b * args.b)['y'].mean().reset_index()
        sns.lineplot(x=mean['x'], y=mean['y'], ax=axes[i, j])
    else:
        mean = df.groupby([filteredDf[args.group], filteredDf['x'] // args.b * args.b])['y'].mean().reset_index()
        sns.lineplot(x=mean['x'], y=mean['y'], hue=mean[args.group], style=mean[args.group], 
                     ax=axes[i, j], palette=palette, hue_order=args.gorder, markers=args.markers,
                     dashes=False, markevery=args.markevery, style_order=args.gorder)

    axes[i, j].grid(color='#eee', linewidth=0.4)

    # title and labels
    if not args.no_title:
        axes[i, j].set_title(operation)
    axes[i, j].set_ylabel('Response time (ms)')
    axes[i, j].set_xlabel('Time (s)')

    # limits
    xlim = axes[i, j].get_xlim()
    ylim = axes[i, j].get_ylim()
    axes[i, j].set_xlim(left=args.xmin if args.xmin is not None else xlim[0], 
                        right=args.xmax if args.xmax is not None else xlim[1])
    axes[i, j].set_ylim(bottom=args.ymin if args.ymin is not None else ylim[0], 
                        top=args.ymax if args.ymax is not None else ylim[1])
    
    # legend
    if axes[i, j].legend_ is not None:
        axes[i, j].legend(loc=args.loc, labelspacing=0.3)
        axes[i, j].legend_.set_title(None)
        axes[i, j].legend_.get_frame().set_linewidth(0)

    j = (j + 1) % args.c
    if j == 0:
        i += 1

plt.tight_layout()

# export
plt.savefig(args.output or f'rt_log.png', dpi=300)
