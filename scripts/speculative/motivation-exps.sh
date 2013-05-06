#!/bin.sh

DATA_DIR="/home/pavlo/Documents/H-Store/Papers/speculative/data"
FABRIC_TYPE="ssh"

# MOTIVATION
for b in smallbank seats tpcc; do
    ./experiment-runner.py $FABRIC_TYPE \
        --exp-type=motivation-singlepartition \
        --results-dir=$DATA_DIR/motivation-singlepartition \
        --partitions=8 \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --no-json

    ./experiment-runner.py $FABRIC_TYPE \
        --exp-type=motivation-dtxn-multinode \
        --results-dir=$DATA_DIR/motivation-dtxn-multinode \
        --partitions=16 \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --no-json
        
    ./experiment-runner.py $FABRIC_TYPE \
        --exp-type=motivation-dtxn-singlenode \
        --results-dir=$DATA_DIR/motivation-dtxn-singlenode \
        --partitions=8 \
        --benchmark=$b \
        --stop-on-error \
        --exp-trials=1 \
        --no-json
done
