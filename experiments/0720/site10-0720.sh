#!/bin/bash
ant clean-all build-all
ant hstore-prepare -Dproject="voterdemohstorewXsYY" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterdemosstorewXsYY" -Dhosts="localhost:0:0"
python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0720/voterdemohstorewXsYY-1c-90-0720-site10.txt" --txnthreshold 0.90 -e "experiments/0720/site10-0720.txt" --winconfig "tuple w100s10 (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0720/voterdemosstorewXsYY-1c-90-0720-site10.txt" --txnthreshold 0.90 -e "experiments/0720/site10-0720.txt" --winconfig "tuple w100s10 (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 5
