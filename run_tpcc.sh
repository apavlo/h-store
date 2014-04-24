#!/usr/bin/zsh
# RUN EXPERIMENTS

DEFAULT_LATENCY=100
LOG_DIR=log
SCRIPT=./experiment.sh 

for ((i=2; i<=8; i*=4))
do
    l=$(($i*$DEFAULT_LATENCY))
    
    echo "---------------------------------------------------"
    echo "LATENCY" $l

    $SCRIPT -s $l &> $LOG_DIR/$i.log

    echo "---------------------------------------------------"

    $SCRIPT -t &>> $LOG_DIR/$i.log

    cp results.csv "$LOG_DIR/$i.csv"
done
