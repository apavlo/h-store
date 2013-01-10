#!/bin/sh

# Re-generate all the message headers
# TODO: Integrate this with the build system.

for i in `find . | grep messages.py$`; do
    i=`echo $i | sed s/^\.\\\\///`
    OUT=`echo $i | sed s/.py/.h/`
    PYTHONPATH=. ./$i $OUT
    echo $i
done
