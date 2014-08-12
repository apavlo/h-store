ant clean-all build-all
ant hstore-prepare -Dproject="voterdemohstorewXsYY" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="voterdemosstorewXsYY" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0812/voterdemohstorewXsYY-1c-90-0812-site01-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 3 --perc_compare
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0812/voterdemosstorewXsYY-1c-90-0812-site01-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare
#python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0812/voterdemosstorewXsYY-1c-90-0812-site01.txt" \
#--txnthreshold 0.90 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
#--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --numruns 1 --perc_compare

python ./tools/autorunexp.py -p "voterdemohstorewXsYY" -o "experiments/0812/voterdemohstorewXsYY-1c-90-0812-site01-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hstore --hscheduler --numruns 3 --perc_compare --log
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0812/voterdemosstorewXsYY-1c-90-0812-site01-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare --log
python ./tools/autorunexp.py -p "voterdemosstorewXsYY" -o "experiments/0812/voterdemosstorewXsYY-1c-90-0812-site01-perc.txt" \
--txnthreshold 0.95 -e "experiments/0812/site01-0812-demosuite-2.txt" --winconfig "tuple w100s10 (site01) perc_compare" \
--threads 1 --rmin 1000 --rstep 1000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare --log --weakrecovery_off