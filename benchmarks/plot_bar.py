import argparse
import sys
import matplotlib as mpl
from matplotlib import ticker
from matplotlib.legend_handler import HandlerPatch
from matplotlib.patches import FancyArrowPatch
import numpy as np
mpl.use('agg')
import matplotlib.pyplot as plt
import pandas as pd
import os
import seaborn as sns
from itertools import product
from matplotlib.transforms import Bbox

# true type
plt.rc('pdf', fonttype=42)

# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('files', nargs='+', type=str, help='File with CSV data to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name (if none is provided, the y axis name will be used)', action='store', required=False)
parser.add_argument('-x', type=str, help='X axis field', action='store', required=True)
parser.add_argument('-y', type=str, help='Y axis field', action='store', required=True)
parser.add_argument('-xname', type=str, help='X axis name', action='store', required=False)
parser.add_argument('-yname', type=str, help='Y axis name', action='store', required=False)
parser.add_argument('-g', '--group', type=str, help='Group lines by field', action='store', default='')
parser.add_argument('-f', '--filter', type=str, help='Filter lines which match some criteria', action='store', required=False)
parser.add_argument('-rx', type=int, help='Rotate x labels in the specified degrees', action='store', default=0)
parser.add_argument('-log', default=False, help='Make the y-axis log-scale', action='store_true')
parser.add_argument('-height', type=float, help='Plot height', action='store', default=4)
parser.add_argument('-width', type=float, help='Width multiplier (based on the number of bars)', action='store', default=0.15)
parser.add_argument('-xorder', nargs='+', type=str, help='Order of the x axis labels', action='store', default='')
parser.add_argument('-gorder', nargs='+', type=str, help='Order of the groups', action='store', default='')
parser.add_argument('-ymin', type=float, help='Y-axis minimum', action='store')
parser.add_argument('-ymax', type=float, help='Y-axis maximum', action='store')
parser.add_argument('-loc', type=str, help='Legend location', action='store', default='best')
parser.add_argument('-ncols', type=int, help='Number of legend cols', action='store', default=1)
parser.add_argument('-palette', type=str, help='Color palette', action='store', default='rocket_r')
parser.add_argument('-colors', nargs='+', type=str, help='List of colors to use', action='store')
parser.add_argument('-saturation', type=float, help='Color saturation', default=1)
parser.add_argument('-hatches', nargs='+', type=str, help='Hatches to use', action='store')
parser.add_argument('-p', type=str, help='Percentile column to use', action='store', required=False)
parser.add_argument('-plabel', type=str, help='Percentile label', action='store', required=False)
parser.add_argument('-ploc', type=str, help='Percentile legend location', default='upper right', action='store', required=False)
parser.add_argument('-nolegend', default=False, help='Export the plot without the legend', action='store_true')
parser.add_argument('-legendonly', default=False, help='Export only the legend', action='store_true')
parser.add_argument('-columnspacing', type=float, help='Spacing between legend columns', action='store', default=1)
parser.add_argument('-yticksdelta', type=float, help='Step between each ytick (-1 for automatic deltas)', action='store', default=-1)
parser.add_argument('-handlelength', type=float, help='Length of the legend handle', action='store', default=None)
parser.add_argument('-handletextpad', type=float, help='Spacing between legend line and label', action='store', default=None)
parser.add_argument('-xhide', default=False, help='Hides the x axis', action='store_true')
parser.add_argument('-yhide', default=False, help='Hides the y axis', action='store_true')
args = parser.parse_args()

validFiles = []
for file in args.files:
    if not os.path.isfile(file):
        print(f'Warning: File {file} does not exist, ignoring it.', file=sys.stderr)
    else:
        validFiles.append(file)

if len(validFiles) == 0:
    exit(f'Warning: No files to read.')

# read csv
df = pd.concat((pd.read_csv(file) for file in validFiles), ignore_index=True)
df['_sep'] = '-' # separator column

if args.filter:
    df = df[df.eval(args.filter)]
df = df.eval("_X = " + args.x)
df = df.eval("_Y = " + args.y)
df = df.eval("_G = " + args.group)
if args.p:
    df = df.eval("_P = " + args.p)

# check if all groups are valid
if args.group and args.gorder:
    notFound = [g for g in args.gorder if g not in df['_G'].values]
    if notFound:
        print(f'Warning: Missing groups from the source data ({notFound}).', file=sys.stderr) 

pivot = df.pivot_table(index='_X', columns='_G', values='_Y', fill_value=0)
pivot = pivot.stack().reset_index(name='_Y')

# size
bars = len(pivot['_X'].unique()) * (max(len(pivot['_X'].unique()), len(args.gorder)) if pivot['_G'] is not None else 1)
figure = plt.figure(figsize=(2 + args.width * bars, args.height))

# color
groups = len(df['_G'].unique())
palette = (
    [sns.desaturate(c, args.saturation) for c in args.colors]
    if args.colors is not None
    else sns.color_palette(args.palette, desat=args.saturation, n_colors=groups)
)

# plot
ax = sns.barplot(data=pivot, x='_X', y='_Y', hue='_G', order=args.xorder or None, palette=palette, saturation=1,
                 errorbar=None, hue_order=args.gorder or None, zorder=2, edgecolor='#000', linewidth=0.4)
ax.grid(axis='y', color='#eee', zorder=0, linewidth=0.4)

# log scale
if args.log:
    ax.set_yscale('log')

# hatches
if args.hatches:
    mpl.rcParams['hatch.linewidth'] = 0.3
    hatches = [e for l in [[args.hatches[i].replace('_', '-')] * len(args.xorder) for i in range(len(args.hatches))] for e in l]

    for i, (hue, x) in enumerate(product(args.gorder, args.xorder)):
        try:
            ax.patches[i].set_hatch(hatches[i])
        except:
            pass

    # legend
    for i, hatch in enumerate(reversed([h.replace('_', '-') for h in args.hatches])):
        try:
            ax.patches[-i - 1].set_hatch(hatch)
        except:
            pass

# legend
if ax.legend_ is not None:
    ax.legend_.set_title(None)
legend = plt.legend(loc=args.loc, labelspacing=0.4, ncols=args.ncols, columnspacing=args.columnspacing, 
                    handlelength=args.handlelength, handletextpad=args.handletextpad)
legend.get_frame().set_linewidth(0)

# percentile
if args.p:
    # handler for the percentile legend
    class HandlerArrow(HandlerPatch):
        def create_artists(self, legend, orig_handle, xdescent, ydescent, width, height, fontsize, trans):
            p = FancyArrowPatch((0, height/2), (width, height/2), arrowstyle='|-|,widthA=0', mutation_scale=2.8)
            self.update_prop(p, orig_handle, legend)
            p.set_transform(trans)
            return [p]
        
    # add percentile legend
    errorbar = FancyArrowPatch((0, 0.1), (1.2, 0), color="#000", linewidth=0.4, label=args.plabel)
    plt.legend(handles=[errorbar], handler_map={FancyArrowPatch : HandlerArrow()}, loc=args.ploc).get_frame().set_linewidth(0)
    ax.add_artist(legend)

    for i, (hue, x) in enumerate(product(args.gorder, args.xorder)):
        try:
            center = ax.patches[i].properties()['center']
            entry = df[(df['_X'] == x) & (df['_G'] == hue)]
            if len(entry) == 1:
                y = entry['_Y'].item()
                p = entry['_P'].item()
            else:
                y = np.mean(entry['_Y'])
                p = np.mean(entry['_P'])

            _, caplines, barlines = plt.errorbar(x=center[0], y=y, yerr=[[0], [p - y if p > y else 0]], fmt='none', label=args.plabel,
                                            ecolor='#000', capsize=0, lolims=True, elinewidth=0.4)
            caplines[0].set_marker('_')
            caplines[0].set_markeredgewidth(0.4)
        except:
            pass

# labels
if args.rx:
    ax.set_xticklabels(ax.get_xticklabels(), rotation=args.rx, ha="right")
ax.set_xlabel(args.xname if args.xname is not None else args.x)
ax.set_ylabel(args.yname if args.yname is not None else args.y)
ax.xaxis.set_tick_params(length=0)

# y limit
if args.ymin:
    plt.ylim(bottom=args.ymin)
if args.ymax:
    plt.ylim(top=args.ymax)

# ticks delta
if args.yticksdelta != -1:
    ax.yaxis.set_major_locator(ticker.MultipleLocator(args.yticksdelta))

# remove legend
if args.nolegend:
    legend.remove()

plt.tight_layout()

# hide axis
if args.xhide:
    ax.tick_params(axis='x', which='both', bottom=False, labelbottom=False)
    ax.set_xlabel('')
if args.yhide:
    ax.tick_params(axis='y', which='both', left=False, labelleft=False)
    ax.set_ylabel('')

# bbox
if args.legendonly:
    legend.get_frame().set_boxstyle('Square', pad=0)
    legend.get_frame().set_alpha(1)
    legend.get_frame().set_edgecolor("black")
    legend.get_frame().set_linewidth(0.8)
    bbox = legend.get_window_extent().transformed(figure.dpi_scale_trans.inverted())
    bbox = Bbox.from_extents(bbox.x0 - 0.012, bbox.y0 - 0.012, bbox.x1 + 0.012, bbox.y1 + 0.012)
else:
    bbox = None

# export
plt.savefig(args.output or ((args.yname or args.y) + '.png'), dpi=300, bbox_inches=bbox)
