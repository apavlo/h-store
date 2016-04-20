#!/bin/bash

#UtilityWorkMessage ---------------------------------------------------------------------

trap onexit 1 2 3 15
function onexit() {
    local exit_status=${1:-$?}
    pkill -f hstore.tag
        exit $exit_status
}

# ---------------------------------------------------------------------

ENABLE_ANTICACHE=true

#SITE_HOST="dev3.db.pdl.cmu.local"
SITE_HOST="localhost"

#CLIENT_HOSTS=( "localhost")

CLIENT_HOSTS=( \
        "localhost" \
        "localhost" \
        "localhost" \
        "localhost" \
        )
N_HOSTS=4
BASE_CLIENT_THREADS=1
CLIENT_THREADS_PER_HOST=4
#BASE_SITE_MEMORY=8192
#BASE_SITE_MEMORY_PER_PARTITION=1024
BASE_SITE_MEMORY=12288
BASE_SITE_MEMORY_PER_PARTITION=2024
BASE_PROJECT="voter"
BASE_DIR=`pwd`
#OUTPUT_DIR_PREFIX="data-sketch/mergeupdate"
#OUTPUT_DIR_PREFIX="data-DRAM/"
#OUTPUT_DIR_PREFIX="data-tpcc-8GB/optimized/"
#OUTPUT_DIR_PREFIX="data-tmp"
#OUTPUT_DIR_PREFIX="data-tm1-3GB/optimized/"
OUTPUT_DIR_PREFIX="data-voter/optimized/"
#OUTPUT_DIR_PREFIX="data-voter/default/"
#OUTPUT_DIR_PREFIX="data-blocksize-throttle-10GB-2/"
#OUTPUT_DIR_PREFIX="data-allocator-10GB/"
#OUTPUT_DIR_PREFIX="data-ALLOCATORNVM-10GB/"
#OUTPUT_DIR_PREFIX="data-blocksize-10GB-HDD/"
#OUTPUT_DIR_PREFIX="data-sketch-125T/"
#BLK_CON=1
#BLK_EVICT=800
AC_THRESH=500
SCALE=100
#BLOCK_SIZE_KB=256
DURATION_S=150
WARMUP_S=3
INTERVAL_S=2
PARTITIONS=8

CPU_SITE_BLACKLIST="1,2,4,6,8,10,12,14"

for BLK_CON in 500; do
for round in 2 3; do
#for DB in 'NVM'; do
for DB in  'CHATHAM' 'SMR'; do
for BLOCK_SIZE in 1024; do
#for DB in 'ALLOCATORNVM'; do
#for device in 'NVM' 'SSD' 'DRAM'; do
for BLOCK_MERGE in 'false'; do
for BLOCKING in 'true';do
    #if [ "$DB" = "HDD" -a "$BLOCK_MERGE" = "false" ]; then
    #    continue
    #fi
#for BLOCK_MERGE in 'false' 'true'; do
        #OUTPUT_DIR=${OUTPUT_DIR_PREFIX}${BLK_EVICT}*${BLOCK_SIZE}kb
        #mkdir -p $OUTPUT_DIR

    #for DB in 'NVM' ; do
    #for latency in 160; do
    #for latency in 160 320 640 1280; do
     #    /data/devel/sdv-tools/sdv-release/ivt_pm_sdv.sh --enable --pm-latency=$latency
    for AC_THRESH in 250; do
    #for AC_THRESH in 9000; do
        #if [ "$DB" = "NVM" ]; then
        #    BLOCK_SIZE=1
        #fi
        if [ "$DB" = "CHATHAM" ]; then
            BLOCK_SIZE=4
        fi
        if [ "$DB" = "SMR" ]; then
            BLOCK_SIZE=16
        fi
    #for DB in 'NVM' 'ALLOCATORNVM' 'SSD'; do
    #for DB in  'SSD' 'NVM' 'HDD' 'DRAM'; do
        for skew in 1.01; do
        #for read_percent in 90 100; do
        for sketch_thresh in 10; do
        #for sketch_thresh in 0 10 40 200; do
            #sed -i "55c\ \ \ \ #define SKETCH_THRESH ${sketch_thresh}" src/ee/anticache/AntiCacheEvictionManager.h
            #ant ee-build

            #OUTPUT_DIR=${OUTPUT_DIR_PREFIX}${device}
            OUTPUT_DIR=${OUTPUT_DIR_PREFIX}${DB}
            #OUTPUT_DIR="${OUTPUT_DIR_PREFIX}${DB}/sketch${sketch_thresh}"
            mkdir -p $OUTPUT_DIR
                echo $BLK_EVICT

                if [ "$BLOCKING" = "true" ]; then
                    block='sync'
                else
                    block='abrt'
                fi
                if [ "$BLOCK_MERGE" = "true" ]; then
                    block_merge='block'
                else
                    block_merge='tuple'
                fi

                #AC_THRESH=500
                DB_TYPE="$DB"
                if [ "$DB" = "NVM" ]; then
                    AC_DIR='/mnt/pmfs/aclevel2'
                else
                    #BLOCK_SIZE=4
                    if [ "$DB" != "ALLOCATORNVM" ]; then
                        DB_TYPE="BERKELEY"
                    fi
                    if [ "$DB" = "SSD" ]; then
                        AC_DIR="/data1/ac_berk/ycsb-berk-level1"
                        #AC_DIR="/data1/ac_berk/ycsb-berk-level$RANDOM"
                    fi
                    if [ "$DB" = "HDD" ]; then
                        AC_DIR="tmp/ac_berk/ycsb-berk-level1"
                        #AC_DIR="tmp/ac_berk/ycsb-berk-level$RANDOM"
                    fi
                    if [ "$DB" = "SMR" ]; then
                        AC_DIR="/smr/ac_berk/ycsb-berk-level1"
                        #AC_DIR="/data1/ac_berk/ycsb-berk-level$RANDOM"
                    fi
                    if [ "$DB" = "CHATHAM" ]; then
                        AC_DIR="/mnt/chatham/ac_berk/ycsb-berk-level1"
                        #AC_DIR="/data1/ac_berk/ycsb-berk-level$RANDOM"
                    fi
                    if [ "$DB" = "DRAM" ]; then
                        AC_THRESH=5000
                    fi
                fi
                rm -rf $AC_DIR

                BLOCK_SIZE_KB=$BLOCK_SIZE
                BLK_EVICT=$((102400 / $BLOCK_SIZE_KB))
                #if [ "$BLOCK_SIZE" = "256" ]; then
                    #BLK_EVICT=$((409600 / $BLOCK_SIZE_KB))
                #    BLOCKING="false"
                    #BLOCK_MERGE="true"
                #fi

                OUTPUT_PREFIX="$OUTPUT_DIR/$round-tm1-3G-$block-$DB-S$skew-${PARTITIONS}p-${BLK_CON}c-${N_HOSTS}h-${CLIENT_THREADS_PER_HOST}ct-sc$SCALE-${BLOCK_SIZE_KB}kb-${BLK_EVICT}b-${AC_THRESH}th-${DURATION_S}s-${block_merge}-R${read_percent}"
                #OUTPUT_PREFIX="$OUTPUT_DIR/$round-ycsb1G-$block-$DB-S$skew-${PARTITIONS}p-${BLK_CON}c-${N_HOSTS}h-${CLIENT_THREADS_PER_HOST}ct-sc$SCALE-${BLOCK_SIZE_KB}kb-${BLK_EVICT}b-${AC_THRESH}th-${DURATION_S}s-${block_merge}"
                LOG_PREFIX="logs/ycsb-nvm/$round-ycsb1G-$block-$DB-S$skew-${PARTITIONS}p-${BLK_CON}c-${N_HOSTS}h-${CLIENT_THREADS_PER_HOST}ct-sc$SCALE-${BLOCK_SIZE_KB}kb-${BLK_EVICT}b-${AC_THRESH}th-${DURATION_S}s-${block_merge}"
                echo "log = $LOG_PREFIX"
                echo $OUTPUT_PREFIX
                sed -i '$ d' "properties/benchmarks/ycsb.properties"
                echo "skew_factor = $skew" >> "properties/benchmarks/ycsb.properties"

#ANTICACHE_BLOCK_SIZE=65536

#ANTICACHE_BLOCK_SIZE=262144
                ANTICACHE_BLOCK_SIZE=$(($BLOCK_SIZE_KB * 1024))
                if [ "$BLOCK_SIZE" = "1" ]; then
                    ANTICACHE_BLOCK_SIZE=$(($BLOCK_SIZE_KB * 1100))
                fi
                ANTICACHE_THRESHOLD=.5
                DURATION=$((${DURATION_S} * 1000))
                WARMUP=$((${WARMUP_S} * 1000))
                INTERVAL=$((${INTERVAL_S} * 1000))

                BASE_ARGS=( \
# SITE DEBUG
                        "-Dsite.status_enable=false" \
                        "-Dsite.status_interval=10000" \
#    "-Dsite.status_exec_info=true" \
#    "-Dsite.status_check_for_zombies=true" \
#    "-Dsite.exec_profiling=true" \
#     "-Dsite.profiling=true" \
#    "-Dsite.txn_counters=true" \
#    "-Dsite.pool_profiling=true" \
#     "-Dsite.network_profiling=false" \
#     "-Dsite.log_backup=true"\
#    "-Dnoshutdown=true" \    

# Site Params
                        "-Dsite.jvm_asserts=false" \
                        "-Dsite.specexec_enable=false" \
                        "-Dsite.cpu_affinity_one_partition_per_core=true" \
#"-Dsite.cpu_partition_blacklist=0,2,4,6,8,10,12,14,16,18" \
#"-Dsite.cpu_utility_blacklist=0,2,4,6,8,10,12,14,16,18" \
                        "-Dsite.network_incoming_limit_txns=100000" \
                        "-Dsite.commandlog_enable=false" \
                        "-Dsite.txn_incoming_delay=5" \
                        "-Dsite.exec_postprocessing_threads=false" \
                        "-Dsite.anticache_eviction_distribution=proportional" \
                        #"-Dsite.log_dir=$LOG_PREFIX" \
                        #"-Dclient.log_dir=$LOG_PREFIX" \
                        "-Dsite.specexec_enable=false" \

#    "-Dsite.queue_allow_decrease=true" \
#    "-Dsite.queue_allow_increase=true" \
    "-Dsite.queue_threshold_factor=0.02" \

                        "-Dsite.cpu_partition_blacklist=${CPU_SITE_BLACKLIST}" \

# Client Params
                        "-Dclient.scalefactor=${SCALE}" \
                        "-Dclient.memory=2048" \
                        "-Dclient.txnrate=16000" \
                        "-Dclient.warmup=${WARMUP}" \
                        "-Dclient.duration=${DURATION}" \
                        "-Dclient.interval=${INTERVAL}" \
                        "-Dclient.shared_connection=false" \
                        "-Dclient.blocking=true" \
                        "-Dclient.blocking_concurrent=${BLK_CON}" \ 
                        #"-Dclient.throttle_backoff=100" \
                        "-Dclient.output_anticache_evictions=${OUTPUT_PREFIX}-evictions.csv" \
                        #"-Dclient.output_anticache_profiling=${OUTPUT_PREFIX}-acprofiling.csv" \
#                       "-Dclient.output_anticache_access=${OUTPUT_PREFIX}-accesses.csv" \
                        "-Dclient.output_memory_stats=${OUTPUT_PREFIX}-memory.csv" \
                        "-Dclient.output_anticache_memory_stats=${OUTPUT_PREFIX}-anticache-memory.csv" \
                        #"-Dclient.weights=\"ReadRecord:${read_percent},UpdateRecord:$((100 - ${read_percent})),*:0\"" \

# Anti-Caching Experiments
                        "-Dsite.anticache_enable=${ENABLE_ANTICACHE}" \
                        "-Dsite.anticache_timestamps=${ENABLE_TIMESTAMPS}" \
                        "-Dsite.anticache_batching=false" \
                        "-Dsite.anticache_profiling=false" \
                        "-Dsite.anticache_reset=false" \
                        "-Dsite.anticache_block_size=${ANTICACHE_BLOCK_SIZE}" \
                        "-Dsite.anticache_check_interval=2000" \
                        "-Dsite.anticache_threshold_mb=${AC_THRESH}" \
                        "-Dsite.anticache_blocks_per_eviction=${BLK_EVICT}" \
                        "-Dsite.anticache_max_evicted_blocks=50000000" \
                        "-Dsite.anticache_dbsize=10000M" \
                        "-Dsite.anticache_db_blocks=$BLOCKING" \
                        "-Dsite.anticache_block_merge=$BLOCK_MERGE" \
                        "-Dsite.anticache_dbtype=$DB_TYPE" \
#    "-Dsite.anticache_evict_size=${ANTICACHE_EVICT_SIZE}" \
                        "-Dsite.anticache_dir=${AC_DIR}" \
                        #"-Dsite.anticache_dir=/mnt/pmfs/aclevel0" \
                        #"-Dsite.anticache_dir=/data1/berk-level$round" \
                        "-Dsite.anticache_threshold=${ANTICACHE_THRESHOLD}" \
                        "-Dclient.anticache_enable=false" \
                        "-Dclient.anticache_evict_interval=2000" \
                        "-Dclient.anticache_evict_size=${ANTICACHE_BLOCK_SIZE}" \
                        "-Dclient.output_csv=${OUTPUT_PREFIX}-results.csv" \

# CLIENT DEBUG
                        "-Dclient.output_txn_counters=${OUTPUT_PREFIX}-txncounters.csv" \
                        "-Dclient.output_clients=false" \
                        "-Dclient.profiling=false" \
                        "-Dclient.output_response_status=false" \
                        "-Dclient.output_queue_profiling=${OUTPUT_PREFIX}-queue.csv" \
                        "-Dclient.output_basepartitions=true" \
#"-Dclient.jvm_args=\"-verbose:gc -XX:+PrintGCDetails -XX:+PrintGCTimeStamps -XX:-TraceClassUnloading\"" 
                        )

                EVICTABLE_TABLES=( \
                    "votes" \
                                )
                EVICTABLES=""
                if [ "$ENABLE_ANTICACHE" = "true" ]; then
                    for t in ${EVICTABLE_TABLES[@]}; do
                        EVICTABLES="${t},${EVICTABLES}"
                    done
                fi

# Compile
                HOSTS_TO_UPDATE=("$SITE_HOST")
                for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
                    NEED_UPDATE=1
                    for x in ${HOSTS_TO_UPDATE[@]}; do
                        if [ "$CLIENT_HOST" = "$x" ]; then
                            NEED_UPDATE=0
                            break
                        fi
                    done
                    if [ $NEED_UPDATE = 1 ]; then
                        HOSTS_TO_UPDATE+=("$CLIENT_HOST")
                    fi
                done
#for HOST in ${HOSTS_TO_UPDATE[@]}; do
#ssh $HOST "cd $BASE_DIR && git pull && ant compile" &
#done
                wait

                ant compile
#HSTORE_HOSTS="${SITE_HOST}:0:0-7"
                if [ "$PARTITIONS" = "1" ]; then
                    HSTORE_HOSTS="${SITE_HOST}:0:0"
                else
                    PART_NO=$((${PARTITIONS} - 1))
                    HSTORE_HOSTS="${SITE_HOST}:0:0-$PART_NO"
                fi
                echo "$HSTORE_HOSTS"

                NUM_CLIENTS=$((${PARTITIONS} * ${BASE_CLIENT_THREADS}))
                SITE_MEMORY=`expr $BASE_SITE_MEMORY + \( $PARTITIONS \* $BASE_SITE_MEMORY_PER_PARTITION \)`

# BUILD PROJECT JAR
                ant hstore-prepare \
                            -Dproject=${BASE_PROJECT} \
                            -Dhosts=${HSTORE_HOSTS} \
                            -Devictable=${EVICTABLES}
                test -f ${BASE_PROJECT}.jar || exit -1

# UPDATE CLIENTS
                CLIENT_COUNT=0
                CLIENT_HOSTS_STR=""
                for CLIENT_HOST in ${CLIENT_HOSTS[@]}; do
                    CLIENT_COUNT=`expr $CLIENT_COUNT + 1`
                    if [ ! -z "$CLIENT_HOSTS_STR" ]; then
                        CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}",
                    fi
                    CLIENT_HOSTS_STR="${CLIENT_HOSTS_STR}${CLIENT_HOST}"
                done

# DISTRIBUTE PROJECT JAR
                for HOST in ${HOSTS_TO_UPDATE[@]}; do
                    if [ "$HOST" != $(hostname) ]; then
                        scp -r ${BASE_PROJECT}.jar ${HOST}:${BASE_DIR} &
                    fi
                done
                wait

                echo "Client count $CLIENT_COUNT client hosts: $CLIENT_HOSTS_STR"
# EXECUTE BENCHMARK
                ant hstore-benchmark ${BASE_ARGS[@]} \
                                -Dproject=${BASE_PROJECT} \
                                -Dkillonzero=false \
                                -Dclient.threads_per_host=${CLIENT_THREADS_PER_HOST} \
                                -Dsite.memory=${SITE_MEMORY} \
                                -Dclient.hosts=${CLIENT_HOSTS_STR} \
                                -Dclient.count=${CLIENT_COUNT}
                result=$?
                if [ $result != 0 ]; then
                    exit $result
                fi
            done
        done
    done
done
done
done
done
done #BLOCK_SIZE
done #BLK_CON
