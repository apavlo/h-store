# H-Store

H-Store is an experimental main-memory, parallel database management system that is
optimized for on-line transaction processing (OLTP) applications. It is a highly
distributed, row-store-based relational database that runs on a cluster on 
shared-nothing, main memory executor nodes. 

More information and documentation is available at: <http://hstore.cs.brown.edu>

## Supported Platforms
H-Store is known to work on the following platforms.
Please note that it will not compile on 32-bit systems.
+ Ubuntu Linux 9.10+ (64-bit)
+ Red Hat Enterprise Linux 5.5 (64-bit)
+ Mac OS X 10.6+ (64-bit)

## Dependencies
+ [gcc +4.3](http://www.ubuntuupdates.org/gcc)
+ [openjdk +1.6](http://www.ubuntuupdates.org/openjdk-7-jdk) (1.7 is recommended)
+ [ant +1.7](http://www.ubuntuupdates.org/ant)
+ [python +2.7](http://www.ubuntuupdates.org/python)
+ [openssh-server](http://www.ubuntuupdates.org/openssh-server) (for automatic deployment)

## Quick Start
1. First build the entire distribution:

        ant build

2. Next make the project jar file for the target benchmark.
   H-Store includes several [benchmarks](http://hstore.cs.brown.edu/doc/deployment/benchmarks/)
   that are built-in and ready to execute. A project jar contains all the of stored 
   procedures and statements for the target benchmark, as well as the cluster 
   configuration for the database system.

        export HSTORE_BENCHMARK=tm1
        ant hstore-prepare -Dproject=$HSTORE_BENCHMARK

3. You can now execute the benchmark locally on your machine with two partitions

        ant hstore-benchmark -Dproject=$HSTORE_BENCHMARK

More information is available here: <http://hstore.cs.brown.edu/doc/quick-start/>
