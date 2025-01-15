# Execution time per run (seconds)
TIME=60
# Seconds to exclude from the beginning of a run
WARMUP=3
# Second to exclude from the end of a run
COOLDOWN=3
# Number of runs per test (each runs for $TIME seconds)
RUNS=3
# Number of clients
WORKERS=1
# List with encodings to test
SCHEMAS=(row array json cube)
# List with number of sites to test
SITES=(1 2 4 6 8 10 12 14 16)
# Number of items
ITEMS=1000
# Number of operations per item
OPS=1000
