#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, commands, string

numHardwareThreads = 4
if CTX.PLATFORM == "Darwin":
    numHardwareThreads = 0
    output = commands.getstatusoutput("sysctl hw.ncpu")
    numHardwareThreads = int(string.strip(string.split(output[1])[1]))
elif CTX.PLATFORM == "Linux":
    numHardwareThreads = 0
    for line in open('/proc/cpuinfo').readlines():
        name_value = map(string.strip, string.split(line, ':', 1))
        if len(name_value) != 2:
            continue
        name,value = name_value
        if name == "processor":
            numHardwareThreads = numHardwareThreads + 1
print numHardwareThreads