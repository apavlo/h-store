#!/bin/bash -x

DB_HOST=istc1
LOCALHOST=`hostname`
BASE_DIR=`pwd`
BASE_PROJECT="voter"
DB_SYSTEM="postgres"

for cpus in `seq 1 10` ; do
    CPU_IDS=""
    for i in `seq 0 $cpus`; do
        if [ "$i" -gt 0 ]; then
            CPU_IDS="${CPU_IDS},"
        fi
        CPU_IDS="${CPU_IDS},${i}"
    done

    PIDS=""
    if [ $DB_SYSTEM = "postgres" ]; then
        PIDS=$(ssh $DB_HOST "ps acx | egrep -i 'postgres -D' | grep pavlo | awk '{print $1}")
    elif [ $DB_SYSTEM = "mysql" ]; then
    
    fi

    for pid in $PIDS ; do
        ssh $DB_HOST "taskset -p -c $CPU_IDS $pid"
    done
    
    ./oltpbenchmark -b $BASE_PROJECT \
        -c config/voter_${DB_SYSTEM}_config.xml \
        --clear=true \
        --create=true \
        --load=true \
        --execute=true \
        -s 5 \
        -o results/${BASE_PROJECT}.${DB_SYSTEM}.${cpus}
    result=$?
    if [ $result != 0 ]; then
        exit $result
    fi
done
