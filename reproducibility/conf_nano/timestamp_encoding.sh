# Execution time per run (seconds)
TIME=1
# Seconds to exclude from the beginning of a run
WARMUP=0
# Second to exclude from the end of a run
COOLDOWN=0
# Number of runs per test (each runs for $TIME seconds)
RUNS=1
# Number of clients
WORKERS=1
# List with encodings to test
SCHEMAS=(row array json cube)
# List with number of sites to test
SITES=(1 16)
# Number of items
ITEMS=100
# Number of operations per item
OPS=100
