#!/bin/bash -x

# ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    for SITE_HOST in ${SITE_HOSTS[@]}; do
        ssh $SITE_HOST "pkill -f hstore.tag" &
    done
    wait
    exit $exit_status
}

# ---------------------------------------------------------------------

SITE_HOSTS=( \
    "istc3"
    "istc4"
    "istc5"
    "istc6"
)
CLIENT_HOSTS=( \
    "istc3" \
#     "istc4" \
#     "istc5" \
#     "vise5" \
#     "vise5" \
)

LOCALHOST=`hostname`
BASE_DIR=`pwd`


PROJECT="tpcc"
PARTITIONS_PER_SITE=8
SCALE_FACTOR=1.0
BASE_SITE_MEMORY=1024
BASE_SITE_MEMORY_PER_PARTITION=2048
CLIENT_TXNRATE=100000
CLIENT_BLOCKING=true
CLIENT_WEIGHTS=""

if [ $PROJECT = "tm1" ]; then
    BASE_SITE_MEMORY=1024
    BASE_SITE_MEMORY_PER_PARTITION=1024
    BASE_CLIENT_THREADS=50
    BASE_CLIENT_CONCURRENT=30
    CLIENT_BLOCKING=false
    CLIENT_TXNRATE=3100
    CLIENT_WEIGHTS="GetAccessData:45,GetNewDestination:10,GetSubscriberData:43,UpdateSubscriberData:2,*:0"
    
elif [ $PROJECT = "voter" ]; then
    BASE_CLIENT_THREADS=4
    BASE_CLIENT_CONCURRENT=5
    CLIENT_BLOCKING=false
    CLIENT_TXNRATE=100000
elif [ $PROJECT = "tpcc" ]; then
    BASE_CLIENT_THREADS=50
    BASE_CLIENT_CONCURRENT=30
    CLIENT_BLOCKING=true
    CLIENT_TXNRATE=15000
else
    BASE_CLIENT_THREADS=50
    BASE_CLIENT_CONCURRENT=32
    CLIENT_BLOCKING=true
    CLIENT_TXNRATE=10000
fi

SPECEXEC_ENABLE=false
MARKOV_ENABLE=false
MARKOV_FIXED=${MARKOV_ENABLE}
MARKOV_DIR="files/markovs/vldb-august2012"
MARKOV_RECOMPUTE=false

BASE_ARGS=( \
    # SITE DEBUG
#     "-Dsite.status_enable=true" \
#     "-Dsite.status_interval=10000" \
#     "-Dsite.status_exec_info=true" \
#    "-Dsite.exec_profiling=true" \
#     "-Dsite.network_profiling=false" \
#     "-Dsite.log_backup=true" \
#    "-Dnoshutdown=true" \
    
    # Site Params
    "-Dsite.jvm_asserts=false" \
    "-Dsite.cpu_affinity_one_partition_per_core=true" \
    "-Dsite.queue_incoming_max_per_partition=20000" \
    "-Dsite.queue_incoming_increase_max=30000" \
    "-Dsite.commandlog_enable=true" \
    "-Dsite.commandlog_timeout=5" \
    "-Dsite.network_txn_initialization=true" \
    "-Dsite.pool_txn_enable=false" \
    
    # Markov Params
    "-Dsite.markov_enable=$MARKOV_ENABLE" \
    "-Dsite.markov_fixed=$MARKOV_FIXED" \
    "-Dsite.markov_singlep_updates=false" \
    "-Dsite.markov_dtxn_updates=false" \
    "-Dsite.markov_path_caching=true" \
    "-Dsite.specexec_enable=${SPECEXEC_ENABLE}" \
    "-Dsite.specexec_idle=${SPECEXEC_ENABLE}" \
    "-Dsite.specexec_unsafe=true" \
    "-Dsite.specexec_unsafe_limit=-1" \
    "-Dsite.exec_mispredict_crash=false" \
#     "-Dsite.exec_force_localexecution=false" \
    
    # Client Params
    "-Dclient.memory=4096" \
    "-Dclient.txnrate=$CLIENT_TXNRATE" \
    "-Dclient.warmup=20000" \
    "-Dclient.duration=60000 "\
    "-Dclient.shared_connection=true" \
    "-Dclient.blocking=$CLIENT_BLOCKING" \
#     "-Dclient.blocking_concurrent=100" \
    "-Dclient.throttle_backoff=100" \
    
    # CLIENT DEBUG
    "-Dclient.profiling=false" \
    "-Dclient.txn_hints=${MARKOV_ENABLE}" \
    "-Dclient.weights=$CLIENT_WEIGHTS" \
#     "-Dclient.output_specexec_profiling=${PROJECT}-specexec.csv" \
    "-Dclient.output_txn_counters=${PROJECT}-txncounters.csv" \
    "-Dclient.output_txn_counters_combine=false" \
#     "-Dclient.output_exec_profiling=${PROJECT}-exec.csv" \

#     "-Dclient.output_markov_profiling=markovprofile.csv" \
#     "-Dclient.output_site_profiling=siteprofile.csv" \
#     "-Dclient.output_txn_counters_combine=true" \
#     "-Dclient.output_basepartitions=true" \
#     "-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" \
)

FILES_TO_COPY=( \
    "${PROJECT}.jar" \
    "log4j.properties" \
    "build.xml" \
    "properties/default.properties" \
    "properties/benchmarks/${PROJECT}.properties" \
)

# for SITE_HOST in ${SITE_HOSTS[@]}; do
#     ssh ${SITE_HOST} "cd ${BASE_DIR} && git pull && ant compile" &
# done
# 
# UPDATED_HOSTS=$SITE_HOSTS
# for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
#     found=0
#     for H in ${UPDATED_HOSTS[@]}; do
#         if [ "$H" = "$CLIENT_HOST" ]; then
#             found=1
#             break
#         fi
#     done
#     if [ $found = 0 -a $CLIENT_HOST != "localhost" ]; then
#         ssh ${CLIENT_HOST} "cd ${BASE_DIR} && git pull && ant compile" &
#         UPDATED_HOSTS=("${UPDATED_HOSTS[@]}" $CLIENT_HOST)
#     fi
# done
# wait

for i in 16 ; do
    if [ "$i" = 1 ]; then
        HSTORE_HOSTS="istc3:0:0"
    elif [ "$i" -le $PARTITIONS_PER_SITE ]; then
        HSTORE_HOSTS="istc3:0:0-$(expr $i - 1)"
    else
        HSTORE_HOSTS=""
        if [ "$i" -ge $PARTITIONS_PER_SITE ]; then
            HSTORE_HOSTS="istc3:0:0-7"
        fi
        if [ "$i" -ge $(expr $PARTITIONS_PER_SITE \* 2) ]; then
            HSTORE_HOSTS="${HSTORE_HOSTS};istc4:1:8-15"
        fi
        if [ "$i" -ge $(expr $PARTITIONS_PER_SITE \* 4) ]; then
            HSTORE_HOSTS="${HSTORE_HOSTS};istc5:2:16-23;istc6:3:24-31"
        fi
    fi
#     if [ $PROJECT = "tm1" ]; then
#         SCALE_FACTOR=$i
#     fi
    
#     NUM_CLIENTS=`expr $i \* $BASE_CLIENT_THREADS`
    NUM_CLIENTS=$BASE_CLIENT_THREADS
    CONCURRENT=`expr $i \* $BASE_CLIENT_CONCURRENT`
    SITE_MEMORY=`expr $BASE_SITE_MEMORY + \( $PARTITIONS_PER_SITE \* $BASE_SITE_MEMORY_PER_PARTITION \)`
    
    # BUILD PROJECT JAR
    ant hstore-prepare \
        -Dproject=${PROJECT} \
        -Dhosts=${HSTORE_HOSTS} \
        -Dpartitionplan=files/designplans/${PROJECT}.lns.pplan \
        -Dpartitionplan.ignore_missing=False 
    test -f ${PROJECT}.jar || exit -1
    
    # BUILD MARKOVS FILE
    MARKOV_FILE="$MARKOV_DIR/${PROJECT}-${i}p.markov.gz"
    if [ $MARKOV_FIXED != "true" -a $MARKOV_ENABLE = "true" -a ! -f $MARKOV_FILE ]; then
        ant markov-generate -Dproject=$PROJECT \
            -Dworkload=files/workloads/$PROJECT.100p-1.trace.gz \
            -Dglobal=false \
            -Dvolt.client.memory=10000 \
            -Doutput=$PROJECT.markov
        gzip --force --best $PROJECT.markov
        mv $PROJECT.markov.gz $MARKOV_FILE
    fi
    
    for file in ${FILES_TO_COPY[@]}; do
        for SITE_HOST in ${SITE_HOSTS[@]}; do
            if [ $SITE_HOST != $LOCALHOST ]; then
                scp $file ${SITE_HOST}:${BASE_DIR}/$file &
            fi
        done
    done
    if [ $MARKOV_FIXED != "true" -a $MARKOV_ENABLE = "true" -a -f $MARKOV_FILE ]; then
        if [ $SITE_HOST != $LOCALHOST ]; then
            scp ${MARKOV_FILE} ${SITE_HOST}:${BASE_DIR}/${MARKOV_FILE} &
        fi
    fi

    # UPDATE CLIENTS
    CLIENT_COUNT=0
    CLIENT_HOSTS_STR=""
    for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
        if [ $CLIENT_HOST != $LOCALHOST -a $CLIENT_HOST != "localhost" ]; then
            for file in ${FILES_TO_COPY[@]}; do
                scp $file ${CLIENT_HOST}:${BASE_DIR}/$file || exit -1
            done
        fi
        CLIENT_COUNT=`expr $CLIENT_COUNT + 1`
        if [ ! -z "$CLIENT_HOSTS_STR" ]; then
            CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR},"
        fi
        CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}${CLIENT_HOST}"
    done
    wait
    
    # EXECUTE BENCHMARK
    ant hstore-benchmark ${BASE_ARGS[@]} \
        -Dproject=${PROJECT} \
        -Dkillonzero=true \
        -Dmarkov.recompute_end=${MARKOV_RECOMPUTE} \
        -Dclient.scalefactor=${SCALE_FACTOR} \
        -Dclient.threads_per_host=${NUM_CLIENTS} \
        -Dclient.blocking_concurrent=${CONCURRENT} \
        -Dsite.memory=${SITE_MEMORY} \
        -Dclient.hosts=${CLIENT_HOSTS_STR} \
        -Dclient.count=${CLIENT_COUNT}
        #         -Dmarkov=${MARKOV_FILE} \
    result=$?
    if [ $result != 0 ]; then
        exit $result
    fi
done
