# Execution time per run (seconds)
TIME=60
# Seconds to exclude from the beginning of a run
WARMUP=3
# Second to exclude from the end of a run
COOLDOWN=3
# Number of runs per test (each runs for $TIME seconds)
RUNS=3
# Number of clients
WORKERS=128
# Number of structures
STRUCTURES=1000000
# List with benchmark types (r - reads; w - writes)
TYPES=(r w)
# List with engines to test
ENGINES="crdv riak"
# Number of sites to test (1..$MAX_SITES)
MAX_SITES=6
