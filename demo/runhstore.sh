#!/bin/bash

ant hstore-benchmark -Dproject=voterdemohstorecorrect -Dclient.threads_per_host=5 -Dclient.txnrate=20 -Dglobal.sstore=false -Dglobal.sstore_scheduler=false -Dclient.duration=1000000