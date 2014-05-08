#!/bin/sh
ant clean-java build-java
ant hstore-prepare -Dproject=voterdemosstorepetrigonly -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemohstorenocleanup -Dhosts="localhost:0:0"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 5000 --rstep 1000 --stop --warmup 40000 -p voterdemohstorenocleanup -o "experiments/0501/site07/voterdemohstorenocleanup-1c-0429.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 5000 --rstep 1000 --stop --warmup 40000 -p voterdemosstore -o "experiments/0501/site07/voterdemosstore-1c-0429.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 1000 --rmax 5000 --rstep 1000 --stop --warmup 40000 -p voterdemosstorepetrigonly -o "experiments/0501/site07/voterdemosstorepetrigonly-1c-0429.txt"
python ./tools/runexperiments.py --tmin 10 --tmax 10 --tstep 1 --rmin 100 --rmax 1000 --rstep 100 --stop --warmup 40000 -p voterdemohstorenocleanup -o "experiments/0501/site07/voterdemohstorenocleanup-10c-0429.txt"
python ./tools/runexperiments.py --tmin 10 --tmax 10 --tstep 1 --rmin 100 --rmax 1000 --rstep 100 --stop --warmup 40000 -p voterdemosstore -o "experiments/0501/site07/voterdemosstore-10c-0429.txt"
python ./tools/runexperiments.py --tmin 10 --tmax 10 --tstep 1 --rmin 100 --rmax 1000 --rstep 100 --stop --warmup 40000 -p voterdemosstorepetrigonly -o "experiments/0501/site07/voterdemosstorepetrigonly-10c-0429.txt"