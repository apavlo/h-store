ant clean-java build-java
ant hstore-prepare -Dproject="microexpftriggerstrig5" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig5" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpftriggerstrig5" -o "experiments/0824/microexpftriggerstrig5-1c-95-0824-site07.txt" \
--txnthreshold 0.95 -e "experiments/0824/site07-0824-proctests.txt" --winconfig "proc called from client" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare

python ./tools/autorunexp.py -p "microexpnoftriggerstrig5" -o "experiments/0824/microexpnoftriggerstrig5-1c-95-0824-site07.txt" \
--txnthreshold 0.95 -e "experiments/0824/site07-0824-proctests.txt" --winconfig "proc called in client, fake trigger" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare

python ./tools/autorunexp.py -p "microexpnoftriggerstrig5" -o "experiments/0824/microexpnoftriggerstrig5-1c-95-0824-site07.txt" \
--txnthreshold 0.95 -e "experiments/0824/site07-0824-proctests.txt" --winconfig "proc called in client, NO fake trigger" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 3 --perc_compare --ftrigger_off