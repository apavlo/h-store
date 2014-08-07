ant clean-all build-all
ant hstore-prepare -Dproject="voterdemohstorewXsYY" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterdemosstorewXsYY" -Dhosts="localhost:0:0"


python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0806/voterdemohstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1

python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0806/voterdemohstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --hstore --hscheduler --numruns 1
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --hscheduler --numruns 1
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10)" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --finalrmin 300 --warmup 10000 --numruns 1

python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0806/voterdemohstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0806/voterdemosstorewXsYY-1c-90-0806-site10.txt" \
--txnthreshold 0.90 -e "experiments/0806/site10-0806-demosuite.txt" --winconfig "tuple w100s10 (site10) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --perc_compare