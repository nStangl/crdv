# Execution time per run (seconds)
TIME=10
# List with the number of clients
WORKERS=(8)
# Number of counters per worker
COUNTERS=100
# Time between each log (milliseconds)
LOG_DELTA=100
# List with engines to test
ENGINES="crdv"
# Time to wait after the run ends to stop logging (seconds)
POST_END_WAIT=2
# Time interval to aggregate the log data by (seconds)
BUCKET_INTERVAL=1
# Whether or not to partition the network
PARTITION_NETWORK=true
# Seconds into the run to start the network partition
PARTITION_NETWORK_START=4
# Duration of the network partition (seconds)
PARTITION_NETWORK_DURATION=2

# CRDV background merge worker configs
MERGE_DELTA=0.05 # seconds
MERGE_BATCH_SIZE=10000
MERGE_PARALLELISM=1
