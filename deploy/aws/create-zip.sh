#!/bin/bash
# Creates a zip with the sql-crdt code, to be used for deploying the cluster.

rm -f sql-crdt.zip
cd ../../
git archive -o sql-crdt.zip --prefix=sql-crdt/ HEAD
mv sql-crdt.zip deploy/aws/
