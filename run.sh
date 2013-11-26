#!/bin/sh
# RUN EXPERIMENTS

DEFAULT_LATENCY=110
LOG_DIR=log

for (( i=1; i<=8; i*=2 ))
do
    l=$(($i*$DEFAULT_LATENCY))
    echo "LATENCY" $l

    #./ycsb.sh -s $l -m 
    ./ycsb.sh -s $l -m &> $LOG_DIR/$i.log

    cp results.csv $LOG_DIR/$i.csv
done
