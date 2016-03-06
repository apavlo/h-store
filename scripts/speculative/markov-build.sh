#!/bin/bash

NUM_PARTITIONS=32

for b in "tpcc" "seats" "smallbank" ; do # "seats" "smallbank" 
    echo $b
    ant hstore-prepare -Dproject=$b -Dhosts=localhost:0:0-$(expr $NUM_PARTITIONS - 1)
    
    if [ $b == "tpcc" ]; then
        workload=files/workloads/$b.${NUM_PARTITIONS}p-1.trace.gz
    else
        workload=files/workloads/$b-1.trace.gz
    fi
    
    for limit in 100 1000 10000 100000; do
        echo $limit
        ant markov-generate -Dproject=$b \
            -Dnumcpus=8 \
            -Dglobal=false \
            -Dworkload=$workload \
            -Doutput=/tmp/$b.markov \
            -Dlimit=$limit
    done
done
