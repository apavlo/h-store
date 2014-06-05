#!/bin/sh
ant clean-java build-java
ant hstore-prepare -Dproject=voterdemohstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstore -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstorenopetrig -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemohstorewinsp1 -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstorewinsp1 -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject=voterdemosstorenopetrigwinsp1 -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p voterdemohstore -o "experiments/0605/voterdemohstore-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstore -o "experiments/0605/voterdemosstore-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorenopetrig -o "experiments/0605/voterdemosstorenopetrig-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemohstorewinsp1 -o "experiments/0605/voterdemohstorewinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorewinsp1 -o "experiments/0605/voterdemosstorewinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorenopetrigwinsp1 -o "experiments/0605/voterdemosstorenopetrigwinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 40000 --numruns 5 

python ./tools/autorunexp.py -p voterdemohstore -o "experiments/0605/voterdemohstore-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstore -o "experiments/0605/voterdemosstore-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorenopetrig -o "experiments/0605/voterdemosstorenopetrig-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemohstorewinsp1 -o "experiments/0605/voterdemohstorewinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorewinsp1 -o "experiments/0605/voterdemosstorewinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
python ./tools/autorunexp.py -p voterdemosstorenopetrigwinsp1 -o "experiments/0605/voterdemosstorenopetrigwinsp1-1c-30w1s1000t-90-0605-site12.txt" --txnthreshold 0.90 -e "experiments/0605/site12-0605.txt" --winconfig "time 30w1s1000t (site12)" --threads 10 --rmin 100 --rstep 100 --finalrstep 10 --warmup 40000 --numruns 5 
