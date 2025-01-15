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
# List with the number of structures
STRUCTURES=(1 1024)
# List with the percentage of writes
WRITE_PERCENTAGES=(100 95 75 50 25 5 0)
