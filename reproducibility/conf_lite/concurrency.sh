# Execution time per run (seconds)
TIME=30
# Seconds to exclude from the beginning of a run
WARMUP=3
# Second to exclude from the end of a run
COOLDOWN=3
# Number of runs per test (each runs for $TIME seconds)
RUNS=1
# Number of clients
WORKERS=64
# List with the number of entries per structure
ENTRIES=(1 4 16 64 256 1024)
# List with engines to test
ENGINES="crdv native pg_crdt electric riak"
