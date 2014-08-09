#!/bin/bash
ant clean-java build-java
ant hstore-prepare -Dproject="voterdemohstorecorrect" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterdemosstorecorrect" -Dhosts="localhost:0:0"

hstoredir="logs/hstoredemoout"
sstoredir="logs/sstoredemoout"
tmphfile="logs/demohstoreout.txt"
tmpsfile="logs/demosstoreout.txt"

votefile="demo/demo-votes.txt"

echo $tmphfile
echo $tmpsfile

numruns=$1
waittime=$2
txnrate=$3

for i in $(eval echo {1..$numruns});
do
	rm $tmphfile
	rm $tmpsfile
	python demo/votefeeder.py -w $waittime -f $votefile &
	ant hstore-benchmark -Dproject="voterdemohstorecorrect" -Dclient.threads_per_host=10 -Dclient.txnrate=$txnrate \
	-Dglobal.sstore=false -Dglobal.sstore_scheduler=false -Dclient.blocking=true
	pkill python
	newhfile=$hstoredir"/tmp_demohstoreout_"$i".txt"
	echo $newhfile
	mv $tmphfile $newhfile
	python demo/votefeeder.py -w $waittime -f $votefile &
	ant hstore-benchmark -Dproject="voterdemosstorecorrect" -Dclient.threads_per_host=10 -Dclient.txnrate=$txnrate \
	-Dglobal.sstore=true -Dglobal.sstore_scheduler=true -Dclient.blocking=true
	pkill python
	newsfile=$sstoredir"/tmp_demosstoreout_"$i".txt"
	echo $newsfile
	mv $tmpsfile $newsfile
done