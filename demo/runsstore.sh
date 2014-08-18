#!/bin/bash

ant hstore-benchmark -Dproject=voterdemosstorecorrect -Dclient.threads_per_host=5 -Dclient.txnrate=20 -Dglobal.sstore=true -Dglobal.sstore_scheduler=true -Dclient.duration=1000000