import sys
import matplotlib as mpl
mpl.use('agg')
from matplotlib import pyplot as plt
import numpy as np

with open(sys.argv[1]) as f:
    data = np.array([float(x) for x in f.readlines()])

# remove outliers
z_scores = (data - np.mean(data)) / np.std(data)
data = data[np.abs(z_scores) < 3]

plt.scatter(list(range(len(data))), data, color='black', alpha=0.02, s=1)
plt.savefig('scatter.png')
