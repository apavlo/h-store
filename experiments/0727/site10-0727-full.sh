ant clean-all build-all
ant hstore-prepare -Dproject="voterdemohstorewXsYY" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterdemosstorewXsYY" -Dhosts="localhost:0:0"

#h-store#h-store no log
python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0727/voterdemohstorewXsYY-1c-90-0727-site10.txt" --txnthreshold 0.90 -e "experiments/0727/site10-0727-full.txt" \
--winconfig "tuple w100s10 (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --hstore --hscheduler --numruns 3
#h-store log
python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0727/voterdemohstorewXsYY-1c-90-0727-site10.txt" --txnthreshold 0.90 -e "experiments/0727/site10-0727-full.txt" \
--winconfig "tuple w100s10 (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --log --hstore --hscheduler --numruns 3

#s-store no log
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0727/voterdemosstorewXsYY-1c-90-0727-site10.txt" --txnthreshold 0.90 -e "experiments/0727/site10-0727-full.txt" \
--winconfig "tuple w100s10 (site10)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --numruns 3
#s-store weak log no scheduler
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0727/voterdemosstorewXsYY-1c-90-0727-site10.txt" --txnthreshold 0.90 -e "experiments/0727/site10-0727-full.txt" \
--winconfig "tuple w100s10 (site10) (note: scheduler off)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --log --hscheduler --numruns 3
#s-store strong log no scheduler
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0727/voterdemosstorewXsYY-1c-90-0727-site10.txt" --txnthreshold 0.90 -e "experiments/0727/site10-0727-full.txt" \
--winconfig "tuple w100s10 (site10) (note: scheduler off)" --threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --log --weakrecovery_off --hscheduler --numruns 3