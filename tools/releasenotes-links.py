#!/usr/bin/env python

import sys
import re

link = '<a href="https://github.com/apavlo/h-store/commit/%s" class="websvn" title="View Commit Information">[%s]</a>'
for key in sys.argv[1:]:
    print link % (key, key[:7])

    