import argparse
import math
import matplotlib as mpl
mpl.use('agg')
from matplotlib.legend_handler import HandlerPatch
from matplotlib.patches import FancyArrowPatch
import matplotlib.pyplot as plt
import pandas as pd
import os
import seaborn as sns
import matplotlib.ticker as ticker
from pathlib import Path
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
parser.add_argument('-g', '--group', type=str, help='Group lines by field', action='store', required=False)
parser.add_argument('-s', '--style', type=str, help='Style lines by field', action='store', required=False)
parser.add_argument('-gorder', nargs='+', type=str, help='Order of the groups', action='store', required=False)
parser.add_argument('-sorder', nargs='+', type=str, help='Order of the styles', action='store', required=False)
parser.add_argument('-f', '--filter', type=str, help='Filter lines which match some criteria', action='store', required=False)
parser.add_argument('-height', type=float, help='Plot height', action='store', default=4)
parser.add_argument('-width', type=float, help='Width multiplier (based on the number of bars)', action='store', default=4)
parser.add_argument('-ymin', type=float, help='Y-axis minimum', action='store', default=0)
parser.add_argument('-ymax', type=float, help='Y-axis maximum', action='store')
parser.add_argument('-xmin', type=float, help='X-axis minimum', action='store')
parser.add_argument('-xmax', type=float, help='X-axis maximum', action='store')
parser.add_argument('-t', '--text', default=False, help='Whether to consider the x axis as text', action='store_true')
parser.add_argument('--log', default=False, help='Make the y-axis log-scale', action='store_true')
parser.add_argument('--xlog', default=False, help='Make the x-axis log-scale', action='store_true')
parser.add_argument('-loc', type=str, help='Legend location', action='store', default='best')
parser.add_argument('-palette', type=str, help='Color palette', action='store', default='rocket_r')
parser.add_argument('-colors', nargs='+', type=str, help='List of colors to use', action='store')
parser.add_argument('-saturation', type=float, help='Color saturation', default=1)
parser.add_argument('-markers', nargs='+', type=str, help='List of markers to use', action='store')
parser.add_argument('-markevery', type=int, help='Delta between each marker', action='store', default=1)
parser.add_argument('-extracol', type=str, help='Extra column to use when needing to concat a prefix/suffix to a numeric column', action='store', default='')
parser.add_argument('-dashes', nargs='+', type=str, help='List of dash tuples to provide to the plot', action='store')
parser.add_argument('-xticksdelta', type=float, help='Step between each xtick (-1 for automatic deltas)', action='store', default=-1)
parser.add_argument('-yticksdelta', type=float, help='Step between each ytick (-1 for automatic deltas)', action='store', default=-1)
parser.add_argument('-ncols', type=int, help='Number of legend cols', action='store', default=1)
parser.add_argument('-lfontsize', type=int, help='Legend font size', action='store', default=10)
parser.add_argument('-handletextpad', type=float, help='Spacing between legend line and label', action='store', default=None)
parser.add_argument('-handlelength', type=float, help='Length of the legend handle', action='store', default=None)
parser.add_argument('-reversezorder', default=False, help='Make the lines zorder respect the gorder', action='store_true')
parser.add_argument('-p', type=str, help='Percentile column to use', action='store', required=False)
parser.add_argument('-plabel', type=str, help='Percentile label', action='store', required=False)
parser.add_argument('-ploc', type=str, help='Percentile legend location', action='store', default='best')
parser.add_argument('-dropna', default=False, help='Ignore NaN rows', action='store_true')
parser.add_argument('-legendonly', default=False, help='Export only the legend', action='store_true')
parser.add_argument('-nolegend', default=False, help='Export the plot without the legend', action='store_true')
parser.add_argument('-rvlines', nargs='+', type=float, help='Add red vertical lines at some ys', action='store', required=False)
parser.add_argument('-bvlines', nargs='+', type=float, help='Add black vertical lines at some ys', action='store', required=False)
parser.add_argument('-fillbetween', type=str, help='Fills the are between the minimum and maximum Y, with the provided color', action='store', required=False)
parser.add_argument('-xbins', type=int, help='Number of x ticks', action='store', required=False)
parser.add_argument('-ybins', type=int, help='Number of x ticks', action='store', required=False)
parser.add_argument('-xhide', default=False, help='Hides the x axis', action='store_true')
parser.add_argument('-yhide', default=False, help='Hides the y axis', action='store_true')
parser.add_argument('-columnspacing', type=float, help='Spacing between legend columns', action='store', default=1)
parser.add_argument('-kxticks', default=False, help='Uses the "k" suffix to shorten tick numbers', action='store_true')
parser.add_argument('-kyticks', default=False, help='Uses the "k" suffix to shorten tick numbers', action='store_true')
parser.add_argument('-yformatter', type=str, help='Y-ticks string formatter', action='store', required=False)
parser.add_argument('-legendtitle', type=str, help='Legend title', action='store', default='')
parser.add_argument('-legendtitleinline', default=False, help='Inlines the legend title with the labels', action='store_true')
parser.add_argument('-xticks', nargs='+', type=float, help='List of xticks to plot', action='store', required=False)
parser.add_argument('-yticks', nargs='+', type=float, help='List of xticks to plot', action='store', required=False)
parser.add_argument('-xminorticks', nargs='+', type=float, help='List of minor xticks to plot', action='store', required=False)
parser.add_argument('-yminorticks', nargs='+', type=float, help='List of minor yticks to plot', action='store', required=False)
parser.add_argument('-rx', type=int, help='Rotate x ticks in the specified degrees', action='store', default=0)

args = parser.parse_args()

validFiles = []
for file in args.files:
    if not os.path.isfile(file):
        print(f'Warning: File {file} does not exist, ignoring it.')
    else:
        validFiles.append(file)

if len(validFiles) == 0:
    exit(f'Error: No valid file to read.')

# read csvs
dfs = []
for file in validFiles:
    try:
        df = pd.read_csv(file)
    except Exception as e:
        print(f'Failed to read file {file}: {e}')
        exit(1)
    df['_file'] = Path(file).stem
    dfs.append(df)
df = pd.concat(dfs, axis=0, ignore_index=True)
if args.dropna:
    df = df.dropna()

df['_sep'] = '-' # separator column
df['_extracol'] = args.extracol # separator column
if args.filter:
    df = df[df.eval(args.filter)]
df = df.eval("_X = " + args.x)
df = df.eval("_Y = " + args.y)
df = df.eval("_HUE = " + (args.group or "''"))
df = df.eval("_STYLE = " + (args.style or "''"))
df = df.eval("_P = " + (args.p or "''"))
df = df.sort_values('_X')

# convert x axis to text
if args.text:
    df['_X'] = df['_X'].astype(str)

# size
figure = plt.figure(figsize=(args.width, args.height))

# color
groups = len(df['_HUE'].unique())
palette = (
    [sns.desaturate(c, args.saturation) for c in args.colors]
    if args.colors is not None
    else sns.color_palette(args.palette, desat=args.saturation, n_colors=groups)
)

# plot
ax = sns.lineplot(data=df, x='_X', y='_Y', hue='_HUE', palette=palette, hue_order=args.gorder,
                  style='_STYLE' if args.style else '_HUE', style_order=args.sorder if args.style else args.gorder,
                  markers=args.markers, markevery=args.markevery, zorder=100,
                  dashes=True if args.style else ([eval(x) for x in args.dashes] if args.dashes else False))
ax.set_axisbelow(True)
ax.grid(color='#eee', linewidth=0.4, which='major', zorder=-1000)
ax.grid(color='#f8f8f8', linewidth=0.2, which='minor', zorder=-1000)

# log scale
if args.log:
    ax.set_yscale('log')
if args.xlog:
    ax.set_xscale('log')

# legend
if ax.legend_ is not None:
    handles, labels = ax.get_legend_handles_labels()
    ax.legend(handles=handles[1:] if args.style else handles, labels=labels[1:] if args.style else labels,
              title=args.legendtitle, alignment='left', loc=args.loc, labelspacing=0.4, ncols=args.ncols, columnspacing=args.columnspacing,
              fontsize=args.lfontsize, handletextpad=args.handletextpad, handlelength=args.handlelength)
    legend = ax.legend_
    legend.get_frame().set_linewidth(0)
    legend.set_zorder(1)

    if args.style:
        for label in legend.get_texts():
            if label.get_text() == args.style or label.get_text() == 'None':
                label.set_text('- - -')
                break

    if args.legendtitleinline:
        c = legend.get_children()[0]
        title = c.get_children()[0]
        hpack = c.get_children()[1]
        c._children = [hpack]
        hpack._children = [title] + hpack.get_children()


# percentile
if args.p:
    for i, (hue, group) in enumerate(df.groupby('_HUE')):
        gIndex = args.gorder.index(hue) if args.gorder else i

        for x, y, p in zip(group['_X'], group['_Y'], group['_P']):
            _, caplines, barlines = plt.errorbar(x=x, y=y, yerr=[[0], [max(p - y, 0)]],
                                                 color=palette[gIndex], alpha=1, lolims=True,
                                                 elinewidth=0.6, zorder=50 + 1/(max(p - y, 0) + 1))
            caplines[0].set_marker(args.markers[gIndex] if args.markers else '_')
            caplines[0].set_markersize(3)
            caplines[0].set_markeredgewidth(0)
            caplines[0].set_zorder(100)

    if args.plabel and legend is not None:
        class HandlerArrow(HandlerPatch):
            def create_artists(self, legend, orig_handle, xdescent, ydescent, width, height, fontsize, trans):
                p = FancyArrowPatch((0, height/2), (width, height/2), arrowstyle='|-|,widthA=0', mutation_scale=2.8)
                self.update_prop(p, orig_handle, legend)
                p.set_transform(trans)
                return [p]
        errorbar = FancyArrowPatch((0, 0.1), (1.2, 0), color="#000", linewidth=0.6, label=args.plabel)
        legend_p = plt.legend(handles=[errorbar], handler_map={FancyArrowPatch : HandlerArrow()}, loc=args.ploc)
        legend_p.get_frame().set_linewidth(0)
        legend_p.set_zorder(1)
        ax.add_artist(legend)

# remove legend
if args.nolegend:
    legend.remove()

# labels
ax.set_xlabel(args.xname if args.xname is not None else args.x)
ax.set_ylabel(args.yname if args.yname is not None else args.y)

# reverse the zorder of the lines, i.e., make first hues (based on gorder) appear on top
if args.reversezorder:
    lines = plt.gca().get_lines()
    for line, zorder in zip(lines, range(len(lines), 0, -1)):
        line.set_zorder(zorder)

# vertical lines
for rline in (args.rvlines if args.rvlines else []):
    plt.axvline(x=rline, linewidth=0.5, linestyle='--', dashes=(4, 4), color="red")
for rline in (args.bvlines if args.bvlines else []):
    plt.axvline(x=rline, linewidth=0.5, linestyle='--', dashes=(4, 4), color="black")

# fill between max and min
if args.fillbetween:
    groupX = df.groupby('_X')['_Y']
    minY, maxY = groupX.min(), groupX.max()
    plt.fill_between(minY.index, minY, maxY, color=args.fillbetween, alpha=0.3)

# ticks delta
if args.xticksdelta != -1:
    ax.xaxis.set_major_locator(ticker.MultipleLocator(args.xticksdelta))
if args.yticksdelta != -1:
    ax.yaxis.set_major_locator(ticker.MultipleLocator(args.yticksdelta))

# bins
if args.xbins:
    xticks = ax.get_xticks()
    ax.set_xticks(xticks[::math.ceil(len(xticks) / args.xbins)])
if args.ybins:
    plt.locator_params(axis='y', nbins=args.ybins)

# manually add a list of ticks
if args.xticks:
    ax.set_xticks(args.xticks)
    if args.xlog:
        minorTicks = [sl for l in [[x * 10**i for x in range(2, 10)] for i in range(int(math.log10(ax.get_ylim()[1])) + 1)] for sl in l]
        minorTicks = [x for x in minorTicks if x < ax.get_ylim()[1]]
        ax.set_xticks(minorTicks, minor=True)
if args.yticks:
    ax.set_yticks(args.yticks)
    if args.log:
        minorTicks = [sl for l in [[x * 10**i for x in range(2, 10)] for i in range(int(math.log10(ax.get_ylim()[1])) + 1)] for sl in l]
        minorTicks = [x for x in minorTicks if x < ax.get_ylim()[1]]
        ax.set_yticks(minorTicks, minor=True)

# ticks format
if args.yformatter:
    ax.yaxis.set_major_formatter(ticker.FormatStrFormatter(args.yformatter))

# kticks
if args.kxticks:
    ticks = ax.get_xticklabels()
    for tick in ticks:
        value = int(tick.get_text().replace('−', '-'))
        if value >= 1000:
            tick.set_text(f'{value // 1000}k')
    ax.set_xticklabels(ticks)
if args.kyticks:
    ticks = ax.get_yticklabels()
    for tick in ticks:
        value = int(tick.get_text().replace('−', '-'))
        if value >= 1000:
            tick.set_text(f'{value // 1000}k')
    ax.set_yticks(ax.get_yticks()) # protects against a warning
    ax.set_yticklabels(ticks)

# ticks rotation
if args.rx:
    plt.xticks(rotation=args.rx)

# y limit
if args.ymin is not None and (args.ymin != 0 or not args.log):
    plt.ylim(bottom=args.ymin)
if args.ymax is not None:
    plt.ylim(top=args.ymax)

# x limit
if args.xmin is not None:
    plt.xlim(left=args.xmin)
if args.xmax is not None:
    plt.xlim(right=args.xmax)

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
plt.savefig(args.output or ((args.yname or args.y) + '.png'), bbox_inches=bbox)
