import argparse
from collections import defaultdict
from dataclasses import dataclass
import os
import json
from typing import Dict
import dateutil.parser


@dataclass
class DataPoint:
    delay: int = 0 # combined delay
    transactions: int = 0 # combined transactions
    delayMeasurements: int = 0 # number of delay measurements


# args
parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument('file', type=str, help='File to process')
parser.add_argument('-o', type=str, help='Output file name', action='store', default='out.csv')
parser.add_argument('-w', type=int, help='Main worker id', action='store', default=0)
parser.add_argument('-b', type=float, help='Time interval to aggregate (0 to disable aggregation)', action='store', default=1)
args = parser.parse_args()

if not os.path.isfile(args.file):
    exit(f'File {args.file} does not exist.')

# key -> latest value
latestValues = defaultdict(int)
# time -> DataPoint
data: Dict[tuple[int, float], DataPoint] = {}
begin = None
intervalStart = None
currNumWorkers = 0

with open(args.file) as f:
    for line in f:
        try:
            entry = json.loads(line)
        except:
            continue
        t = dateutil.parser.parse(entry["time"])

        # start time
        if entry["message"] == "Running" and begin is None:
            begin = t

        # run started
        elif entry["message"] == "Run started":
            currNumWorkers = entry["workers"]
            latestValues.clear()

        # transaction completed
        elif entry["message"] == "completed":
            assert begin is not None
            t = dateutil.parser.parse(entry["real_time"])
            timeDelta = (t - begin).total_seconds()
            timeBucket = (timeDelta // args.b) * args.b if args.b != 0 else timeDelta

            if (currNumWorkers, timeBucket) not in data:
                data[(currNumWorkers, timeBucket)] = DataPoint()

            data[(currNumWorkers, timeBucket)].transactions += 1

        # delay measurement
        elif entry["message"] == "read":
            # main worker measurement
            if entry["worker"] == args.w:
                assert begin is not None
                timeDelta = (t - begin).total_seconds()
                timeBucket = (timeDelta // args.b) * args.b if args.b != 0 else timeDelta
                totalCounters = entry["totalCounters"]

                if (currNumWorkers, timeBucket) not in data:
                    data[(currNumWorkers, timeBucket)] = DataPoint()

                # update the delay
                if len(latestValues) > 0:
                    data[(currNumWorkers, timeBucket)].delay += sum([max(latest - (entry[key] if key in entry else 0), 0)
                                        for key, latest in latestValues.items()]) * totalCounters / len(latestValues)
                data[(currNumWorkers, timeBucket)].delayMeasurements += 1

            # other measurements
            else:
                for key, value in entry.items():
                    if key.startswith('_'):
                        latestValues[key] = max(latestValues[key], value)

# write to csv
with open(args.o, 'w') as f:
    f.write('time,workers,delay,tps\n')
    avgDelay = 0
    for (workers, time), info in sorted(data.items()):
        # average delay: if there is no measurement, use the last one
        avgDelay = info.delay / info.delayMeasurements if info.delayMeasurements > 0 else avgDelay
        tps = info.transactions / args.b
        prevTime = time
        f.write(f'{time},{workers},{avgDelay},{tps}\n')
