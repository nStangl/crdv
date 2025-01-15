import argparse
import dateutil
import matplotlib as mpl
mpl.use('agg')
import matplotlib.pyplot as plt
import json
import os
import pandas as pd
import seaborn as sns

# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('file', type=str, help='File with JSON log to plot')
parser.add_argument('-o', '--output', type=str, help='Output file name (if none is provided, re_log.png will be used)', action='store', required=False)
parser.add_argument('-height', type=float, help='Plot height', action='store', default=1.9)
parser.add_argument('-width', type=float, help='Plot width', action='store', default=5)
parser.add_argument('-f', type=int, help='Filter by site id', action='store')
args = parser.parse_args()

if not os.path.isfile(args.file):
    exit(f'File {args.file} does not exist.')

# read json
data = []
begin = None

with open(args.file) as f:
    for line in f:
        entry = json.loads(line)
        t = dateutil.parser.parse(entry["time"])
        if entry["message"] == "Running":
            begin = t
        elif begin is not None and entry["message"] == "Unmerged rows":
            if args.f is None or entry["connId"] == args.f:
                ts = (t - begin).total_seconds()
                data.append({'site': entry["connId"], 'time': ts, 'count': entry["count"]})

df = pd.DataFrame(data)

# plot
plt.figure(figsize=(args.width, args.height))
ax = sns.lineplot(data=df, x=df['time'], y=df['count'], hue=df['site'], palette='Set2')

# labels
ax.set_xlabel("Time (s)")
ax.set_ylabel("Unmerged rows")

plt.tight_layout()
plt.savefig(args.output or f'unmerged_rows.png', dpi=300)
