import sys, argparse
import os, datetime
import re
import math
from array import *
import fileinput

parser = argparse.ArgumentParser(description='This is a replace script, made by hawk.')
parser.add_argument('-f','--filename', help='filename')
parser.add_argument('-s','--source', help='source')
parser.add_argument('-t','--target', help='target')

args = parser.parse_args()

filename = args.filename
sourcestring = args.source
targetstring  = args.target

with open(filename) as f:
    file_str = f.read()
#
file_str = file_str.replace(sourcestring, targetstring)
#
with open(filename, 'w') as f:
    f.write(file_str)
