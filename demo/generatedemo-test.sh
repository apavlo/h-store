#!/bin/bash

ant clean-java build-java
ant hstore-prepare -Dproject=generatedemo -Dhosts="localhost:0:0"
rm demo/demo-votes-2.txt
rm demo/demo-answer-2.txt
ant hstore-benchmark -Dproject=generatedemo -Dclient.threads_per_host=1 -Dclient.txnrate=500 -Dclient.blocking=true -Dclient.duration=1000000