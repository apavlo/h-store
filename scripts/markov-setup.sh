#!/bin/bash -x

BENCHMARKS=( \
#    "tm1" \
    "tpcc.100w.large" \
    "auctionmark.large"\
    "tpce" \
)
PARTITIONS=( \
#     8 \
#     16 \
#     32 \
#     64 \
    128 \
)
HEAP_SIZE=3072
MAX_THREADS=2
MAKE_GLOBAL=true
CALCULATE_COST=true

WORKLOAD_BUILD_SIZE=50000
WORKLOAD_BUILD_MULTIPLIER=500
WORKLOAD_TEST_SIZE=50000
WORKLOAD_TEST_OFFSET=0
WORKLOAD_TEST_MULTIPLIER=500
MARKOV_FILES_DIR=files/markovs/vldb-feb2011

tm1_mix="DeleteCallForwarding:2,GetAccessData:35,GetNewDestination:10,GetSubscriberData:35,InsertCallForwarding:2,UpdateLocation:14,UpdateSubscriberData:2"
tpce_mix="BrokerVolume:5,CustomerPosition:13,MarketFeed:1,MarketWatch:18,SecurityDetail:14,TradeLookup:8,TradeOrder:10,TradeResult:10,TradeStatus:19,TradeUpdate:2,DataMaintenance:1,TradeCleanup:1"
tpcc_mix="delivery:4,neworder:45,slev:4,ostatByCustomerId:2,ostatByCustomerName:3,paymentByCustomerIdC:13,paymentByCustomerIdW:13,paymentByCustomerNameC:8,paymentByCustomerNameW:9"
auctionmark_mix="CheckWinningBids:-1,GetItem:35,GetUserInfo:10,GetWatchedItems:10,NewBid:13,NewComment:2,GetComment:2,NewCommentResponse:1,NewFeedback:5,NewItem:10,NewPurchase:2,NewUser:8,PostAuction:-1,UpdateItem:2"

for BENCHMARK in ${BENCHMARKS[@]}; do
    BUILD_WORKLOAD=$BENCHMARK
    TEST_WORKLOAD=${BENCHMARK}-2
    
    if [ "$BENCHMARK" = "tpcc.100w" -o "$BENCHMARK" = "tpcc.50w" -o "$BENCHMARK" = "tpcc.100w.large" ]; then
        BENCHMARK="tpcc"
        WORKLOAD_MIX=$tpcc_mix
    elif [ "$BENCHMARK" = "tpce" ]; then
        WORKLOAD_MIX=$tpce_mix
        WORKLOAD_TEST_OFFSET=75000
        TEST_WORKLOAD=$BUILD_WORKLOAD
    elif [ "$BENCHMARK" = "auctionmark.large" ]; then
        BENCHMARK="auctionmark"
        WORKLOAD_MIX=$auctionmark_mix
    elif [ "$BENCHMARK" = "tm1" ]; then
        WORKLOAD_MIX=$tm1_mix
    fi
    if [ -n "$1" -a $1 != $BENCHMARK ]; then
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
            
        for GLOBAL in "true" "false" ]; do
            if [ $GLOBAL = "true" ]; then
                if [ $MAKE_GLOBAL != "true" ]; then
                    continue
                fi
                MARKOV_FILE=$MARKOV_FILES_DIR/$BENCHMARK.global.${NUM_PARTITIONS}p.markovs
            else
                MARKOV_FILE=$MARKOV_FILES_DIR/$BENCHMARK.${NUM_PARTITIONS}p.markovs
            fi
            if [ -f ${MARKOV_FILE}.gz ]; then
                MARKOV_FILE=${MARKOV_FILE}.gz
            fi
    
            if [ ! -f $MARKOV_FILE ]; then
                ant markov \
                    -Dvolt.client.memory=$HEAP_SIZE \
                    -Dproject=$BENCHMARK \
                    -Dworkload=files/workloads/$BUILD_WORKLOAD.trace.gz \
                    -Dlimit=$WORKLOAD_BUILD_SIZE \
                    -Dmultiplier=$WORKLOAD_BUILD_MULTIPLIER \
                    -Dinclude=$WORKLOAD_MIX \
                    -Doutput=$MARKOV_FILE \
                    -Dglobal=$GLOBAL || exit
            fi
            
            ## MarkovCostModel
            if [ $CALCULATE_COST = "true" ]; then
                ant markov-cost \
                    -Dproject=$BENCHMARK \
                    -Dvolt.client.memory=$HEAP_SIZE \
                    -Dhstore.max_threads=$MAX_THREADS \
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