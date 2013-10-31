#!/bin/bash

DB_HOST=istc1
DB_SYSTEM="mysql"
PROJECT="tpcc"
BASE_CLIENTS=10
TRIALS=3

for cpus in `seq 1 8` ; do
    NUM_CLIENTS=$(expr $cpus \* $BASE_CLIENTS)
    CPU_IDS=""
    for i in `seq 0 $(expr $cpus - 1)`; do
        if [ "$i" -gt 0 ]; then
            CPU_IDS="${CPU_IDS},"
        fi
        CPU_IDS="${CPU_IDS}${i}"
    done

    PIDS=""
    if [ $DB_SYSTEM = "postgres" ]; then
        PIDS=$(ssh $DB_HOST "ps aux | egrep -i 'postgres -D' | grep -v grep | grep pavlo | awk '{print \$2}'")
    elif [ $DB_SYSTEM = "mysql" ]; then
        PIDS=$(ssh $DB_HOST "ps aux | egrep -i 'mysqld' | grep -v mysqld_safe | grep -v grep | grep pavlo | awk '{print \$2}'")
        PSTREE=$(ssh $DB_HOST "pstree -A -p $PIDS")
        for line in $PSTREE ; do 
            nextPID=$(echo $line | perl -ne 'print "$1 " if /.*\{mysqld\}\(([\d]+)\).*/')
            PIDS="$PIDS $nextPID"
        done
    fi
    for pid in $PIDS ; do
        ssh $DB_HOST "taskset -p -c $CPU_IDS $pid"
    done
    
    CONFIG_FILE="config/${PROJECT}_${DB_SYSTEM}_config.xml"
    OUTPUT_FILE="results/${PROJECT}_${DB_SYSTEM}_${cpus}"

#     perl -pi -e "s/(<terminals>)[\s]*[\d]+[\s]*(<\/terminals>)/\$1 ${NUM_CLIENTS} \$2/" $CONFIG_FILE
    perl -pi -e "s/(<scalefactor>)[\s]*[\d]+[\s]*(<\/scalefactor>)/\$1 ${cpus} \$2/" $CONFIG_FILE
    
    for trial in `seq 1 $TRIALS`; do
        echo "TRIAL #${trial}"
        if [ $trial = 1 ]; then
            CREATE="true"
        else
            CREATE="false"
        fi
    
        ./oltpbenchmark -b $PROJECT \
            -c ${CONFIG_FILE} \
            --clear=false \
            --create=$CREATE \
            --load=$CREATE \
            --execute=true \
            -s 5 \
            -o ${OUTPUT_FILE}
        result=$?
        if [ $result != 0 ]; then
            exit $result
        fi
    done
done
