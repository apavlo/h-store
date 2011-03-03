#!/bin/sh
# Script to rewrite libevent's include paths to make it work with our funny build system.

set -e

# These are "compiled" internal files
FILES=`echo *.c *-internal.h include/event2/*.h`
perl -pi -e 's{"event2/}{"libevent/include/event2/};' \
        -e 's{<event2/([^>]+)>}{"libevent/include/event2/\1"};' \
        -e 's{<sys/queue.h>}{"libevent/compat/sys/queue.h"};' $FILES
