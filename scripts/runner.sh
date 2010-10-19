#!/bin/bash

## ---------------------------------------
## General Configuration
## ---------------------------------------
ANT="ant"
ANT_TARGET=""
BENCHMARK="tpcc"
CWD=`pwd`

## Volt Benchmarks
TPCC_BENCHMARK_CLIENT="org.voltdb.benchmark.tpcc.TPCCClient"
MULTISITE_BENCHMARK_CLIENT="org.voltdb.benchmark.multisite.MultisiteClient"
BINGO_BENCHMARK_CLIENT="org.voltdb.benchmark.bingo.BingoClient"

## Brown Benchmarks
TM1_BENCHMARK_CLIENT="edu.brown.benchmark.tm1.TM1Client"
AUCTIONMARK_BENCHMARK_CLIENT="edu.brown.benchmark.auctionmark.AuctionMarkClient"
AIRLINE_BENCHMARK_CLIENT="edu.brown.benchmark.airline.Client"
TPCE_BENCHMARK_CLIENT="edu.brown.benchmark.tpce.TPCEClient"
AFFINITY_BENCHMARK_CLIENT="edu.brown.benchmark.affinity.Client"
MARKOV_BENCHMARK_CLIENT="edu.brown.benchmark.markov.MarkovClient"
HELLOWORLD_BENCHMARK_CLIENT="edu.brown.benchmark.helloworld.HelloWorldClient"

EXTRA_ARGS=""
HOSTNAME=`hostname -f`

RANDOM_GENERATOR="org.voltdb.benchmark.tpcc.AffinityRandomGenerator"
RANDOM_PROFILE="./workloads/tpcc.affine.rand"
RANDOM_SEED=0

BUILD_JAVA=0
BUILD_ENGINE=0
BUILD_VOLTBIN=1
RUN_BENCHMARK=1
DELETE_BENCHMARK_JAR=1
FIX_BENCHMARK_JAR=0
COMPILE_ONLY=0
KILL_PROTODTXN=0
SKEW_FACTOR=0.0

## ---------------------------------------
## Default Configuration
## ---------------------------------------
DEFAULT_COORDINATOR_HOST="localhost"
DEFAULT_NUM_HOSTS=1
DEFAULT_NUM_CLIENTS=1
DEFAULT_CLIENT_HOST="localhost"
DEFAULT_NUM_CLIENTPROCESSES=1
DEFAULT_HOST_MEMORY=1024
DEFAULT_CLIENT_MEMORY=512
DEFAULT_SITES_PER_HOST=1
DEFAULT_NUM_WAREHOUSES=2
DEFAULT_SCALE_FACTOR=20000
DEFAULT_DURATION=60000
DEFAULT_TXN_RATE=10000

## ---------------------------------------
## Brown Configuration
## ---------------------------------------
BROWN_COORDINATOR_HOST="localhost"
BROWN_NUM_HOSTS=1
BROWN_NUM_CLIENTS=1
BROWN_CLIENT_HOST="localhost"
BROWN_NUM_CLIENTPROCESSES=1
BROWN_HOST_MEMORY=1024
BROWN_CLIENT_MEMORY=512
BROWN_SITES_PER_HOST=1
BROWN_NUM_WAREHOUSES=10
BROWN_SCALE_FACTOR=1000
BROWN_DURATION=60000
BROWN_TXN_RATE=50

## ---------------------------------------
## Wisconsin Configuration
## ---------------------------------------
WISC_COORDINATOR_HOST="d-10.cs.wisc.edu"
WISC_NUM_PARTITIONS=1
WISC_NUM_CLIENTS=1
WISC_NUM_CLIENTPROCESSES=1
WISC_FIRST_HOST_IDX=11 # first client
WISC_HOST_MEMORY=2048
WISC_CLIENT_MEMORY=2048
WISC_SITES_PER_HOST=2
WISC_NUM_WAREHOUSES=`expr $WISC_NUM_PARTITIONS \* $WISC_SITES_PER_HOST`
WISC_SCALE_FACTOR=2000
WISC_DURATION=60000
WISC_TXN_RATE=1

## ---------------------------------------
## Args
## ---------------------------------------
for arg in $@; do
    if [ "$arg" = "clean" ]; then
        $ANT clean
    ## Execute just the benchmark client
    elif [ "$arg" = "client" ]; then
        BUILD_JAVA=0
        BUILD_EE=0
        BUILD_VOLTBIN=0
        DELETE_BENCHMARK_JAR=0
        EXTRA_ARGS="$EXTRA_ARGS -Dcompile=false"
    elif [ "$arg" = "killprotodtxn" ]; then
        KILL_PROTODTXN=1
    elif [ "$arg" = "debug" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dbuild=debug"
    elif [ "$arg" = "compileonly" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dcompileonly=true -Dhost1=localhost"
        COMPILE_ONLY=1
    elif [ "$arg" = "nocompile" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dcompile=false"
        DELETE_BENCHMARK_JAR=0
    elif [ "$arg" = "nodataload" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dnodataload=true"
    elif [ "$arg" = "blocking" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dblocking=true"
    elif [ "$arg" = "cataloghosts" ]; then
        EXTRA_ARGS="$EXTRA_ARGS -Dcataloghosts=true"
    elif [ "$arg" = "fix" ]; then
        FIX_CATALOG_JAR=1
        ANT_TARGET="catalog-fix"
    elif [ "$arg" = "affine" ]; then
        EXTRA_ARGS="$EXTRA_ARGS \
            -Drandomgenerator=$RANDOM_GENERATOR \
            -Drandomprofile=$RANDOM_PROFILE \
            -Drandomseed=$RANDOM_SEED "
    elif [ "$arg" = "intrusive" ]; then
        EXTRA_ARGS="$EXTRA_ARGS \
            -Duseprofile=intrusive \
            -Dworkload.trace.class=edu.brown.workload.WorkloadTraceFileOutput \
            -Dworkload.trace.path=./workloads/server.trace \
            -Dworkload.trace.ignore=LoadWarehouse,LoadWarehouseReplicated "
    ## Benchmark Types
    elif [ "$arg" = "tpcc" ]; then
        BENCHMARK="TPCC"
    elif [ "$arg" = "tm1" ]; then
        BENCHMARK="TM1"
    elif [ "$arg" = "auctionmark" ]; then
        BENCHMARK="AUCTIONMARK"
        EXTRA_ARGS="$EXTRA_ARGS \
            -Dauctionmarkdir=$CWD/tests/frontend/edu/brown/benchmark/auctionmark/data "
    elif [ "$arg" = "markov" ]; then
        BENCHMARK="MARKOV"
#         DEFAULT_SCALE_FACTOR=5000
    elif [ "$arg" = "helloworld" ]; then
	    BENCHMARK="HELLOWORLD"
    elif [ "$arg" = "airline" ]; then
        BENCHMARK="AIRLINE"
        EXTRA_ARGS="$EXTRA_ARGS \
             -Dairline_data_dir=$CWD/tests/frontend/edu/brown/benchmark/airline/data "
    elif [ "$arg" = "multisite" ]; then
        BENCHMARK="MULTISITE"
    elif [ "$arg" = "bingo" ]; then
        BENCHMARK="BINGO"
    elif [ "$arg" = "tpce" ]; then
        BENCHMARK="TPCE"
        EXTRA_ARGS="$EXTRA_ARGS \
             -Degenloader_home=$CWD/obj/release/tpceloader "
    elif [ "$arg" = "affinity" ]; then
        BENCHMARK="AFFINITY"
    
    ## General Param
    else
        param_key=""
        param_value=""
        for item in `echo $arg | perl -n -e 'print join(" ", split(/=/, $_, 2));'`; do
            if [ -z "$param_key" ]; then
                param_key=$item
            else
                param_value=$item
            fi
        done
        if [ -z "$param_key" -o -z "$param_value" ]; then
            echo "ERROR: Invalid parameter string '$arg'"
            echo "param_key: $param_key"
            echo "param_value: $param_value"
            exit 1
        fi
        eval $param_key="$param_value"
    fi
done

if [ "$BUILD_ENGINE" = 1 ]; then
    ANT_TARGET="ee $ANT_TARGET"
elif [ "$BUILD_JAVA" = 1 ]; then
    ANT_TARGET="compile $ANT_TARGET"
fi
if [ "$RUN_BENCHMARK" = 1 ]; then
    ANT_TARGET="$ANT_TARGET benchmark"
fi

## ---------------------------------------
## Brown
## ---------------------------------------
if [ `echo $HOSTNAME | grep -c cs.brown.edu` = "1" ]; then
    DOMAIN="BROWN"
    EXTRA_ARGS="$EXTRA_ARGS -Dloadthreads=1 -Dclienthost1=localhost"
## ---------------------------------------
## Wisconsin
## ---------------------------------------
elif [ `echo $HOSTNAME | grep -c cs.wisc.edu` = "1" ]; then
    DOMAIN="WISC"
    
    ## Clients
    ctr=1
    while [ $ctr -le $WISC_NUM_CLIENTS ]; do
        host_idx=`expr $WISC_FIRST_HOST_IDX + $ctr - 1`
        EXTRA_ARGS="$EXTRA_ARGS -Dclienthost$ctr="`printf "d-%02d" $host_idx`
        ctr=`expr $ctr + 1`
    done
    
    ## Hosts
#     ctr=1
#     HOSTS=""
#     add=""
#     while [ $ctr -le $WISC_NUM_HOSTS ]; do
#         host_idx=`expr $WISC_FIRST_HOST_IDX + $WISC_NUM_CLIENTS + $ctr - 1`
#         HOSTS="$HOSTS$add"`printf "d-%02d" $host_idx`
#         add=","
#         ctr=`expr $ctr + 1`
#     done


    NUM_HOSTS=1
    EXTRA_ARGS="$EXTRA_ARGS -Dhostcount=1 -Dhost1=$WISC_COORDINATOR_HOST "

## ---------------------------------------
## Default
## ---------------------------------------
else
    DOMAIN="DEFAULT"
    EXTRA_ARGS="$EXTRA_ARGS -Dclienthost1=localhost"
fi

PARAMS=( \
    "COORDINATOR_HOST"\
    "NUM_HOSTS"\
    "SITES_PER_HOST"\
    "NUM_WAREHOUSES"\
    "HOST_MEMORY"\
    "NUM_CLIENTS"\
    "NUM_CLIENTPROCESSES"\
    "CLIENT_MEMORY"\
    "SCALE_FACTOR"\
    "TXN_RATE"\
    "CLIENT_HOST"\
    "DURATION" \
)
for param in ${PARAMS[@]}; do
    eval $param="\${${DOMAIN}_$param}"
done

## ---------------------------------------
## Benchmark Configuration
## ---------------------------------------
PARAMS=( \
    "BENCHMARK_CLIENT"\
)
for param in ${PARAMS[@]}; do
    eval $param="\${${BENCHMARK}_$param}"
done
BENCHMARK_JAR=`echo $BENCHMARK | tr "[:upper:]" "[:lower:]"`.jar

## ---------------------------------------
## Fix Catalog
## ---------------------------------------
if [ "$FIX_CATALOG_JAR" = 1 ]; then
    $ANT $ANT_TARGET $EXTRA_ARGS -Djar=$BENCHMARK_JAR
    exit $?
fi

## ---------------------------------------
## Bombs Away!
## ---------------------------------------
if [ -f $BENCHMARK_JAR ]; then
    if [ "$DELETE_BENCHMARK_JAR" = "1" ]; then
        rm $BENCHMARK_JAR
    fi
else
    EXTRA_ARGS="$EXTRA_ARGS -Dcompile=true "
fi

if [ "$BUILD_VOLTBIN" = "1" ]; then
    if [ -d $HOME/voltbin ]; then
        rm -rf $HOME/voltbin
     fi
     $ANT $EXTRA_ARGS voltbin voltbincopy || exit
     ln -s `pwd`/workloads ~/voltbin/workloads
fi

# -Dskewfactor=0.0 \
if [ -n "$ANT_TARGET" ]; then
    echo "Executing '$ANT_TARGET'..."
    $ANT $ANT_TARGET \
        $EXTRA_ARGS \
        -Dduration=$DURATION \
        -Dvolt.server.memory=$HOST_MEMORY \
        -Dvolt.client.memory=$CLIENT_MEMORY \
        -Dclient=$BENCHMARK_CLIENT \
        -Dclientcount=$NUM_CLIENTS \
        -Dprocessesperclient=$NUM_CLIENTPROCESSES \
        -Dsf=$SCALE_FACTOR \
        -Dtxnrate=$TXN_RATE \
        -Dwarehouses=$NUM_WAREHOUSES \
        -Dloadthreads=1 \
        -Dskew=$SKEW_FACTOR
fi
if [ $KILL_PROTODTXN = 1 ]; then
    killall protodtxnengine
    killall protodtxncoordinator
fi
exit