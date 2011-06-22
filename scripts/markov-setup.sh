#!/bin/bash -x

BENCHMARKS=( \
#     "tm1" \
    "tpcc.100w.large" \
#     "auctionmark.large"\
#     "tpce" \
)
PARTITIONS=( \
    8 \
#     16 \
#     32 \
#     64 \
#     128 \
)
HEAP_SIZE=3072
MAX_THREADS=`tools/getcpus.py`
MAKE_GLOBAL=true
CALCULATE_COST=true
COMPRESS=true
FORCE=false

WORKLOAD_BUILD_SIZE=75000
WORKLOAD_BUILD_MULTIPLIER=500
WORKLOAD_TEST_SIZE=50000
WORKLOAD_TEST_OFFSET=0
WORKLOAD_TEST_MULTIPLIER=500
MARKOV_FILES_DIR=files/markovs/vldb-june2011

TM1_MIX="DeleteCallForwarding:2,GetAccessData:35,GetNewDestination:10,GetSubscriberData:35,InsertCallForwarding:2,UpdateLocation:14,UpdateSubscriberData:2"
TPCE_MIX="BrokerVolume:5,CustomerPosition:13,MarketFeed:1,MarketWatch:18,SecurityDetail:14,TradeLookup:8,TradeOrder:10,TradeResult:10,TradeStatus:19,TradeUpdate:2,DataMaintenance:1,TradeCleanup:1"
TPCC_MIX="delivery:4,neworder:45,slev:4,ostatByCustomerId:2,ostatByCustomerName:3,paymentByCustomerId:26,paymentByCustomerName:17"
AUCTIONMARK_MIX="CheckWinningBids:-1,GetItem:35,GetUserInfo:10,GetWatchedItems:10,NewBid:13,NewComment:2,GetComment:2,NewCommentResponse:1,NewFeedback:5,NewItem:10,NewPurchase:2,NewUser:8,PostAuction:-1,UpdateItem:2"

for arg in $@; do
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
done

for BENCHMARK in ${BENCHMARKS[@]}; do
    BUILD_WORKLOAD=$BENCHMARK
    TEST_WORKLOAD=${BENCHMARK}-2
    
    if [ "$BENCHMARK" = "tpcc.100w" -o "$BENCHMARK" = "tpcc.50w" -o "$BENCHMARK" = "tpcc.100w.large" ]; then
        BENCHMARK="tpcc"
        WORKLOAD_MIX=$TPCC_MIX
    elif [ "$BENCHMARK" = "tpce" ]; then
        WORKLOAD_MIX=$TPCE_MIX
        WORKLOAD_TEST_OFFSET=75000
        TEST_WORKLOAD=$BUILD_WORKLOAD
    elif [ "$BENCHMARK" = "auctionmark.large" ]; then
        BENCHMARK="auctionmark"
        WORKLOAD_MIX=$AUCTIONMARK_MIX
    elif [ "$BENCHMARK" = "tm1" ]; then
        WORKLOAD_MIX=$TM1_MIX
    fi
    if [ -n "$TARGET" -a "$TARGET" != $BENCHMARK ]; then
        continue
    fi

    ## Build project jar
    if [ ! -f "${BENCHMARK}.jar" ]; then
        ant hstore-prepare -Dproject=$BENCHMARK
    fi
        
    for NUM_PARTITIONS in ${PARTITIONS[@]}; do
        ant catalog-fix catalog-info \
            -Dproject=$BENCHMARK \
            -Dnumhosts=$NUM_PARTITIONS \
            -Dnumsites=1 \
            -Dnumpartitions=1 \
            -Dcorrelations=files/correlations/${BENCHMARK}.correlations || exit
            
        if [ "$BENCHMARK" = "tpcc" ]; then
            BUILD_WORKLOAD="vldb-mar2011/tpcc.${NUM_PARTITIONS}p"
        fi
            
        for GLOBAL in "true" "false" ; do
            if [ $GLOBAL = "true" ]; then
                if [ $MAKE_GLOBAL != "true" ]; then
                    continue
                fi
                MARKOV_FILE=$MARKOV_FILES_DIR/$BENCHMARK.${NUM_PARTITIONS}p.global.markovs
            else
                MARKOV_FILE=$MARKOV_FILES_DIR/$BENCHMARK.${NUM_PARTITIONS}p.clustered.markovs
            fi
            if [ -f ${MARKOV_FILE}.gz -a "$FORCE" != true ]; then
                MARKOV_FILE=${MARKOV_FILE}.gz
            fi
    
            if [ ! -f $MARKOV_FILE -o "$FORCE" = true ]; then
                ant markov \
                    -Dvolt.client.memory=$HEAP_SIZE \
                    -Dhstore.max_threads=$MAX_THREADS \
                    -Dproject=$BENCHMARK \
                    -Dworkload=files/workloads/$BUILD_WORKLOAD.trace.gz \
                    -Dlimit=$WORKLOAD_BUILD_SIZE \
                    -Dmultiplier=$WORKLOAD_BUILD_MULTIPLIER \
                    -Dinclude=$WORKLOAD_MIX \
                    -Doutput=$MARKOV_FILE \
                    -Dglobal=$GLOBAL || exit
                if [ "$COMPRESS" = true ]; then
                    gzip --force --best $MARKOV_FILE
                    MARKOV_FILE=${MARKOV_FILE}.gz
                fi
            fi
            
            ## MarkovCostModel
            if [ $CALCULATE_COST = true ]; then
                ant markov-cost \
                    -Dvolt.client.memory=$HEAP_SIZE \
                    -Dhstore.max_threads=$MAX_THREADS \
                    -Dproject=$BENCHMARK \
                    -Dworkload=files/workloads/$TEST_WORKLOAD.trace.gz \
                    -Dlimit=$WORKLOAD_TEST_SIZE \
                    -Dmultiplier=$WORKLOAD_TEST_MULTIPLIER \
                    -Dinclude=$WORKLOAD_MIX \
                    -Dmarkov=$MARKOV_FILE|| exit
            fi
        done # GLOBAL
    done # PARTITIONS
    
    echo "++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
done # BENCHMARK