#!/bin/sh
ant build-java
ant hstore-prepare -Dproject=voterwinhstorenocleanup -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwinsstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwintimehstorenocleanup -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterwintimesstorewinonly -Dhosts="localhost:0:0"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 25000 --rstep 1000 --stop --warmup 40000 -p voterwinhstorenocleanup -o "experiments/0429/site07/voterwinhstorenocleanupW100S10-0429.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 25000 --rstep 1000 --stop --warmup 40000 -p voterwinsstore -o "experiments/0429/site07/voterwinsstoreW100S10-0429.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 25000 --rstep 1000 --stop --warmup 40000 -p voterwintimehstorenocleanup -o "experiments/0429/site07/voterwintimehstorenocleanupW30S2T1000-0429.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 25000 --rstep 1000 --stop --warmup 40000 -p voterwintimesstorewinonly -o "experiments/0429/site07/voterwintimesstorewinonlyW30S2T1000-0429.txt"
