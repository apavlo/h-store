#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os, sys, re, string, commands

jvmVersion = None
output = commands.getoutput("java -version")

regex = re.compile("java version \"(1\.\d)\..*?\"", re.IGNORECASE)
m = regex.search(output)
if m: jvmVersion = m.group(1)

assert not jvmVersion is None
print jvmVersion
