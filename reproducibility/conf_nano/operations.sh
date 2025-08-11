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
# Number of structures
STRUCTURES=1000
# Number of entries per structure
ENTRIES=100
# List with engines to test
ENGINES="crdv native pg_crdt electric riak"
