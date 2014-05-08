#!/bin/sh
ant clean-java build-java
ant hstore-prepare -Dproject=voterdemohstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstore -Dhosts="localhost:0:0"

python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 500 --rmax 8000 --rstep 500 --warmup 70000 -p voterdemohstore -o "experiments/0506/voterdemohstore-1c-10w2s1000t-0506.txt"
python ./tools/runexperiments.py --tmin 1 --tmax 1 --tstep 1 --rmin 500 --rmax 8000 --rstep 500 --warmup 70000 -p voterdemosstore -o "experiments/0506/voterdemosstore-1c-10w2s1000t-0506.txt"
python ./tools/runexperiments.py --tmin 10 --tmax 10 --tstep 1 --rmin 50 --rmax 800 --rstep 50 --warmup 70000 -p voterdemohstore -o "experiments/0506/voterdemohstore-10c-10w2s1000t-0506.txt"
python ./tools/runexperiments.py --tmin 10 --tmax 10 --tstep 1 --rmin 50 --rmax 800 --rstep 50 --warmup 70000 -p voterdemosstore -o "experiments/0506/voterdemosstore-10c-10w2s1000t-0506.txt"