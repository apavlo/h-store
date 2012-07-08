# TPC-E Benchmark

This is a pure Java port of the OSDL's [DBT-5](http://sourceforge.net/apps/mediawiki/osdldbt/) benchmark for MySQL.
Both the data loader and the workload generator are implemented, but there are some deviations in the stored procedure
logic due to limitations in the SQL supported by H-Store.


## Authors

The following people have contributed to this project:

* Andy Pavlo
* Zhe Zhang
* Alex Kalinin
* Wenfeng Xu


## Notes

1. The loader works completely and generates the same data as the original one.

2. All clients currently work as CE. That means MEE-style transactions are not executed, and all MEE computations are
ignored. send_to_market() requests from transactions come with transaction replies, but are ignored by clients.


## Known Issues

1. The `DataMaintenance` transaction is not executed currently. The original specification states that this transaction
needs to be executed periodically.

2. Every procedure's Java file contains a section named *H-Store Quirks*, where I described all deviations from the
specification.

3. Some procedures do not work because of the "100MB" limit exception from the EE. This is due to limitations in
H-Store's distributed query planner.

4. `SecurityDetail.getInfo1` SQL statement does not work, since the planner throws an exception. I changed it with a
much simpler one. Semantically, the transaction works correctly but may not be as efficient. 

5. Originally, in almost all transactions a lot of parameters are retrieved and passed as results in many frames.
However, only small part of them is actually used and passed as a final result. We do the retrieval with SQL queries,
but ignore them after that.