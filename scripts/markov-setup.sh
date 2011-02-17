#!/bin/bash -x

BENCHMARKS=( \
#    "tm1" \
    "tpcc.100w.large" \
#     "auctionmark"\
)
PARTITIONS=( \
#     8 \
#     16 \
#     32 \
#     64 \
    128 \
)
HEAP_SIZE=3072
WORKLOAD_SIZE=50000
WORKLOAD_MULTIPLIER=500


tm1_mix="DeleteCallForwarding:2,GetAccessData:35,GetNewDestination:10,GetSubscriberData:35,InsertCallForwarding:2,UpdateLocation:14,UpdateSubscriberData:2"
tpce_mix="BrokerVolume:5,CustomerPosition:13,MarketFeed:1,MarketWatch:18,SecurityDetail:14,TradeLookup:8,TradeOrder:10,TradeResult:10,TradeStatus:19,TradeUpdate:2,DataMaintenance:1,TradeCleanup:1"
tpcc_mix="delivery:4,neworder:45,slev:4,ostatByCustomerId:2,ostatByCustomerName:3,paymentByCustomerIdC:13,paymentByCustomerIdW:13,paymentByCustomerNameC:8,paymentByCustomerNameW:9"
auctionmark_mix="CheckWinningBids:-1,GetItem:35,GetUserInfo:10,GetWatchedItems:10,NewBid:13,NewComment:2,GetComment:2,NewCommentResponse:1,NewFeedback:5,NewItem:10,NewPurchase:2,NewUser:8,PostAuction:-1,UpdateItem:2"

## -----------------------------------------------------
## buildMarkovs
## -----------------------------------------------------
buildMarkovs() {
    BENCHMARK=$1
    WORKLOAD=$2
    OUTPUT=$3
    GLOBAL=$4
    WORKLOAD_MIX=$5
    ant markov \
        -Dvolt.client.memory=$HEAP_SIZE \
        -Dproject=$BENCHMARK \
        -Dworkload=files/workloads/$WORKLOAD.trace.gz \
        -Dlimit=$WORKLOAD_SIZE \
        -Dmultiplier=$WORKLOAD_MULTIPLIER \
        -Dinclude=$WORKLOAD_MIX \
        -Doutput=$OUTPUT \
        -Dglobal=$GLOBAL || exit
}

for BENCHMARK in ${BENCHMARKS[@]}; do
    WORKLOAD=$BENCHMARK
    
    if [ "$BENCHMARK" = "tpcc.100w" -o "$BENCHMARK" = "tpcc.50w" -o "$BENCHMARK" = "tpcc.100w.large" ]; then
        BENCHMARK="tpcc"
        WORKLOAD_MIX=$tpcc_mix
    elif [ "$BENCHMARK" = "tpce" ]; then
        WORKLOAD_MIX=$tpce_mix
    elif [ "$BENCHMARK" = "auctionmark" ]; then
        WORKLOAD_MIX=$auctionmark_mix
    elif [ "$BENCHMARK" = "tm1" ]; then
        WORKLOAD_MIX=$tm1_mix
    fi

    ## Build project jar
    ant hstore-prepare -Dproject=$BENCHMARK
        
    for NUM_PARTITIONS in ${PARTITIONS[@]}; do
        ant catalog-fix catalog-info \
            -Dproject=$BENCHMARK \
            -Dnumhosts=$NUM_PARTITIONS \
            -Dnumsites=1 \
            -Dnumpartitions=1 \
            -Dcorrelations=files/correlations/${BENCHMARK}.correlations || exit
            
        for GLOBAL in "true" "false" ]; do
            if [ $GLOBAL = "true" ]; then
                OUTPUT=files/markovs/$BENCHMARK.global.${NUM_PARTITIONS}p.markovs
            else
                OUTPUT=files/markovs/$BENCHMARK.${NUM_PARTITIONS}p.markovs
            fi
    
            if [ ! -f $OUTPUT ]; then
                buildMarkovs $BENCHMARK $WORKLOAD $OUTPUT $GLOBAL $WORKLOAD_MIX
            fi
        done # GLOBAL
    done # PARTITIONS
done # BENCHMARK