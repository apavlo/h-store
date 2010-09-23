#!/usr/bin/env bash

set -o errexit -o nounset

top="$( dirname "$( readlink -f "$0" )" )/.."
script=tools/hz.bash
cd "$top"
# Go up one directory to get from the JRE dir to the JDK dir.
JAVA_HOME="$( java -cp tools/ SystemPropertyPrinter java.home )/.."

: ${sf:=1} ${primary:=zs.ath.cx} ${secondary:=harvard.csail.mit.edu} \
  ${ctlport:=23895} ${jvmpid:=$( pgrep java )} ${jsamp_port:=9897}

#
# general
#

join-paths() {
  echo "$@" | tr ' ' :
}

with-classpath() {
  CLASSPATH="$( join-paths obj/release/prod third_party/jars/*.jar ):$JAVA_HOME/lib/tools.jar" "$@"
}

with-ldpath() {
  LD_LIBRARY_PATH="$( java -cp tools/ SystemPropertyPrinter java.library.path ):obj/release/nativelibs:${LD_LIBRARY_PATH:+:}${LD_LIBRARY_PATH:-}" "$@"
}

hzjava() {
  with-ldpath with-classpath java -Xshare:off "$@"
}

hprof() {
  hzjava -agentlib:hprof=cpu=samples,depth=8 "$@"
}

junit() {
  hzjava org.junit.runner.JUnitCore "$@"
}

hzrun() {
  with-ldpath with-classpath MIT_ACR/EE/src/hzrun "$@"
}

gdb-hzrun() {
  local gdbargs=( gdb )

  while (( $# > 0 )) ; do
    local arg="$1"
    shift
    if [[ "$arg" == '--' ]]
    then break
    else gdbargs=( "${gdbargs[@]}" "$arg" )
    fi
  done

  with-ldpath with-classpath "${gdbargs[@]}" --args MIT_ACR/EE/src/hzrun "$@"
}

pgrep-jvm() {
  pgrep java | xargs ps | grep "$@"
}

log-level() {
  local level="$1"
  cp GPL_VCR/log4j-options/log4j-"$level".xml \
     GPL_VCR/gpl_java/build/test/log4j.xml
}

#
# mid-level
#

profile-benchmark() {
  hprof edu.mit.tpcc.BenchmarkMain "$@"
}

benchmark() {
  log-level release
  hzjava -server -XX:+AggressiveOpts -XX:CompileThreshold=1 edu.mit.tpcc.BenchmarkMain "$@"
}

benchmarks() {
  local dir=bench-results sf=10 duration=10

  mkdir -p $dir

  for sites in 1 2 ; do
    for warehouses in  1 2 ; do
      for clients in $(seq 2 $((3*sites))) ; do
        hzjava edu.mit.tpcc.BenchmarkMain $sf $sites $warehouses $clients $duration \
          >& $dir/sf-$sf-sites-$sites-warehouses-$warehouses-clients-$clients-duration-$duration
      done
    done
  done

  for field in 'Total transactions: ' 'New Order transactions: ' 'Transactions/second'
  do fgrep "$field" $dir/*
  done
}

force-compile-tpcc() {
  hzjava edu.mit.tpcc.TPCCCompiler 2 1 $primary
}

compile-tpcc() {
  if [ ! -f tpcc.jar ]
  then force-compile-tpcc
  fi
}

tpcc-leader() {
  hzjava com.horizontica.ServerThread tpcc.jar true
}

tpcc-follower() {
  hzjava com.horizontica.ServerThread tpcc.jar false
}

tpcc-loader() {
  hzjava edu.mit.tpcc.Loader $primary 2 $sf
}

tpcc-clients() {
  local nclients=$1
  hzjava edu.mit.tpcc.Client $primary 2 $sf $nclients
}

secondary() {
  ssh $secondary "hz/MIT_ACR/scripts/hz.bash $@"
}

tcp-probe() {
  # equivalent to (but more portable than): nc -q1 "$@"
  tcpprobe.py "$@"
}

# copy modified sources from localhost to secondary
sync-hosts() {
  for i in $( svn st | grep -v ^? | cut -c2- )
  do scp $i $secondary:hz/$i
  done
}

wait-hz() {
  echo -n 'waiting for hz to start'
  while ! tcp-probe localhost $ctlport ; do
    echo -n .
    sleep 1
  done
}

start-tpcc-leader() {
  tpcc-leader >& /tmp/leader &
  wait-hz
  echo 'started tpcc leader'
}

start-tpcc-follower() {
  tpcc-follower >& /tmp/follower &
  wait-hz
  echo 'started tpcc follower'
}

start-multihost() {
  scp $script $secondary:hz/$script
  compile-tpcc
  secondary compile-tpcc
  start-tpcc-leader
  secondary start-tpcc-follower
}

stop-hz() {
  echo kill | nc -q1 localhost $ctlport
}

stop-multihost() {
  secondary stop-hz
  stop-hz
}

multihost() {
  start-multihost
  tpcc-loader
  tpcc-clients
  stop-multihost
}

start-jsamp() {
  hzjava ca.evanjones.JSamplerAttach $jvmpid 0 10 $jsamp_port \
      jsamp-attach.txt
}

stop-jsamp() {
  tcp-probe localhost $jsamp_port
}

jsamp-postproc() {
  jsamp-postproc.py --exclude edu.mit.tpcc.Client --exclude ca.evanjones "$@"
}

#
# specific
#

profile-benchmark-multithreads() {
  profile-benchmark 10 2 2 3 10
}

benchmark-multithreads() {
  benchmark 10 2 2 3 10
}

gdb-benchmark() {
  gdb-hzrun -ex 'handle SIGSEGV nostop' \
            -ex 'run' \
            -- edu.mit.tpcc.BenchmarkMain
}

#
# main
#

eval "$@"

# vim:et:sw=2:ts=2
