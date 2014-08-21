ant clean-java build-java
ant hstore-prepare -Dproject="microexpftriggerstrig1" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig1" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpftriggerstrig2" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig2" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpftriggerstrig3" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig3" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpftriggerstrig4" -Dhosts="localhost:0:0"
ant hstore-prepare -Dproject="microexpnoftriggerstrig4" -Dhosts="localhost:0:0"

python ./tools/autorunexp.py -p "microexpftriggerstrig1" -o "experiments/0820/microexpftriggerstrig1-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callproc params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig2" -o "experiments/0820/microexpftriggerstrig2-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callproc no params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig3" -o "experiments/0820/microexpftriggerstrig3-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callstreamproc params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpftriggerstrig4" -o "experiments/0820/microexpftriggerstrig4-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callstreamproc no params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare

python ./tools/autorunexp.py -p "microexpnoftriggerstrig1" -o "experiments/0820/microexpnoftriggerstrig1-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callproc params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpnoftriggerstrig2" -o "experiments/0820/microexpnoftriggerstrig2-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callproc no params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpnoftriggerstrig3" -o "experiments/0820/microexpnoftriggerstrig3-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callstreamproc params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
python ./tools/autorunexp.py -p "microexpnoftriggerstrig4" -o "experiments/0820/microexpnoftriggerstrig4-1c-95-0820-vm.txt" \
--txnthreshold 0.95 -e "experiments/0820/vm-0820-proctests.txt" --winconfig "callstreamproc no params" \
--threads 1 --rmin 10000 --rstep 10000 --finalrstep 100 --warmup 10000 --hscheduler --numruns 1 --perc_compare
