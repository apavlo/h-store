#!/bin/bash

MEMORY=3600
TXN_MULTIPLIER=100
TXN_LIMIT=10000
TXN_OFFSET=0

BENCHMARKS=( \
#     "tm1" \
    "tpcc.100w" \
#     "tpcc.50w" \
#     "auctionmark" \
#     "tpce" \
)
PARTITIONPLANS=( \
    "lns" \
    "pkey" \
    "greedy" \
    "popular" \
)

TM1_MIX="DeleteCallForwarding:2,GetAccessData:35,GetNewDestination:10,GetSubscriberData:35,InsertCallForwarding:2,UpdateLocation:14,UpdateSubscriberData:2"
TPCE_MIX="BrokerVolume:5,CustomerPosition:13,MarketFeed:1,MarketWatch:18,SecurityDetail:14,TradeLookup:8,TradeOrder:10,TradeResult:10,TradeStatus:19,TradeUpdate:2,DataMaintenance:1,TradeCleanup:1"
TPCC_MIX="delivery:4,neworder:45,slev:4,ostatByCustomerId:2,ostatByCustomerName:3,paymentByCustomerId:26,paymentByCustomerName:17"
AUCTIONMARK_MIX="CheckWinningBids:-1,GetItem:35,GetUserInfo:10,GetWatchedItems:10,NewBid:13,NewComment:2,GetComment:2,NewCommentResponse:1,NewFeedback:5,NewItem:10,NewPurchase:2,NewUser:8,PostAuction:-1,UpdateItem:2"

for BENCHMARK in ${BENCHMARKS[@]}; do
    WORKLOAD=$BENCHMARK
    if [ "$BENCHMARK" = "tpcc.100w" -o "$BENCHMARK" = "tpcc.50w" ]; then
        BENCHMARK="tpcc"
        TXN_MIX=$TPCC_MIX
    elif [ "$BENCHMARK" = "tpce" ]; then
        TXN_MIX=$TPCE_MIX
    elif [ "$BENCHMARK" = "auctionmark" ]; then
        TXN_MIX=$AUCTIONMARK_MIX
    elif [ "$BENCHMARK" = "tm1" ]; then
        TXN_MIX=$TM1_MIX
    else
        echo "ERROR: Invalid BENCHMARK '$BENCHMARK'"
        exit 1
    fi

    for PARTITIONPLAN in ${PARTITIONPLANS[@]}; do
        PARTITIONPLAN_FILE="./files/designs/$WORKLOAD.$PARTITIONPLAN.pplan"
        if [ ! -f $PARTITIONPLAN_FILE ]; then
            echo "ERROR: Missing $BENCHMARK $PARTITIONPLAN files '$PARTITIONPLAN_FILE'"
            exit 1
        fi
        echo $PARTITIONPLAN_FILE
        ant designer-estimate \
            -Dvolt.client.memory=${MEMORY} \
            -Djar=${BENCHMARK}-designer-benchmark.jar \
            -Dtype=${BENCHMARK} \
            -Dworkload=./files/workloads/${WORKLOAD}.trace.gz \
            -Dlimit=$TXN_LIMIT \
            -Doffset=$TXN_OFFSET \
            -Dinclude=$TXN_MIX \
            -Dexclude=LoadWarehouseReplicated,LoadWarehouse \
            -Dmultiplier=$TXN_MULTIPLIER \
            -Dpartitionplan=$PARTITIONPLAN_FILE \
            -Dextraparams=""
        if [ $? != 0 ]; then
            exit 1
        fi
        echo
    done
done
exit