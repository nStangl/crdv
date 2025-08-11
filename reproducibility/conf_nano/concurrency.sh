# Execution time per run (seconds)
TIME=1
# Seconds to exclude from the beginning of a run
WARMUP=0
# Second to exclude from the end of a run
COOLDOWN=0
# Number of runs per test (each runs for $TIME seconds)
RUNS=1
# Number of clients
WORKERS=64
# List with the number of entries per structure
ENTRIES=(1 1024)
# List with engines to test
ENGINES="crdv"
