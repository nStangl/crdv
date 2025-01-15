#!/bin/bash

# earlyoom is used specifically due to the freshness tests with pg_crdt,
# which use large amounts of memory. if the machine does not have enough
# memory, earlyoom prevents the entire system from becoming unresponsive
# by stopping the benchmark, unlike the standard oom killer.
# more info: https://github.com/rfjakob/earlyoom
service earlyoom start
bash
