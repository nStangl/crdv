# Execution time per run (seconds)
TIME=30
# Seconds to exclude from the beginning of a run
WARMUP=3
# Second to exclude from the end of a run
COOLDOWN=3
# Number of runs per test (each runs for $TIME seconds)
RUNS=1
# Number of clients
WORKERS=1
# Total number of key-value pairs
TOTAL_ENTRIES=100000
# List with the number of key-value pairs for each structure
ENTRIES_PER_STRUCTURE=(1 2 4 8 16 32 64 128)
# List with engines to test
ENGINES="crdv native pg_crdt electric riak"
