#!/usr/bin/python

import os, sys

def walk(numSites, lenComp):
    for i in range(1, numSites):
        newLines = []
        fName = 'SP2_'+str(i)+'.java'
        lines = open(fName, 'r').readlines()
        for line in lines:
            if lenComp == 0:
                if line.strip().startswith('compute();'):
                    line = '// ' + line
            else:
                if line.strip().startswith('Thread.sleep('):
                    line = ' '*12 + 'Thread.sleep(' + str(lenComp) + ');  // Sleep ' + str(lenComp) + ' milliseconds\n';
                if line.startswith('//'):
                    uncommentedLine = line[2:]
                    if uncommentedLine.strip().startswith('compute();'):
                        line = ' '*8 + 'compute();\n'
            newLines.append(line)
        open(fName, 'w').writelines(newLines)
            

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: ' + sys.argv[0] + ' <Number_of_Sites> <Length_of_Computation(ms)>')
        exit(1)
        
    numSites = int(sys.argv[1])
    lenComp = int(sys.argv[2])
    
    walk(numSites, lenComp)
    