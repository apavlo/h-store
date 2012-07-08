#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys
import re
import string

table_regex = re.compile("CREATE TABLE ([A-Z\_]+)[\s]+\(", re.IGNORECASE)
col_regex = re.compile("[\s]+([a-z\_]+)[\s]+([A-Z]+).*?(?:REFERENCES[\s]+([A-Z\_]+)[\s]+\([\s]*([a-z\_]+)[\s]*\))?[\s]*,")

headers = [ 'Column', 'Type', 'Cardinality', 'References', 'Description' ]
for line in sys.stdin:
    match = table_regex.match(line)
    if match:
        table_name = match.group(1)
        columns = [ ]
        for line in sys.stdin:
            match = col_regex.match(line)
            if match:
                attributes = match.groups()
                columns.append(map(lambda x: string.replace(x, '_', '\_') if x else None, attributes))
            else:
                break
        ## FOR
        
        ## Latex Output
        #print '%', table_name
        print '\\subsubsection{\\texttt{%s}}' % table_name.replace('_', '\_')
        
        print """
\\begin{tabular}{ll}
Number of Records:      & TODO \\\\
Average Tuple Size:     & TODO \\\\
Total Size:             & TODO \\\\
\\end{tabular}

\\vspace*{0.1in}
""";
        
        print '\\noindent \\begin{tabular*}{0.75\\textwidth}{@{\\extracolsep{\\fill}} lllll}'
        print ' & '.join([ '\\textbf{%s}' % col for col in headers ]) + " \\\\"
        for col in columns:
            try:
                col_name = "%-18s" % col[0]
                col_type = "%-10s" % col[1]
                col_fkey = "%-15s" % ("%s.%s" % (col[2], col[3]) if col[2] else '-')
                print " & ".join([col_name, col_type, 'TODO', col_fkey, '-' ]) + " \\\\"
            except:
                print "Failed to output:", col
                raise
        ## FOR
        print "\\end{tabular*}"
        print
    ## IF (table)
## FOR