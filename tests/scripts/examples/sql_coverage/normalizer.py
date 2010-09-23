#!/usr/bin/env python

# This file is part of VoltDB.
# Copyright (C) 2008-2009 VoltDB L.L.C.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

#!/usr/bin/env python

import decimal
import re

__EXPR = re.compile(r"ORDER BY \w+\.(\w+)")

VOLTTYPE_NULL = 1
VOLTTYPE_TINYINT = 3  # int8
VOLTTYPE_SMALLINT = 4 # int16
VOLTTYPE_INTEGER = 5  # int32
VOLTTYPE_BIGINT = 6   # int64
VOLTTYPE_FLOAT = 8    # float64
VOLTTYPE_STRING = 9
VOLTTYPE_TIMESTAMP = 11 # 8 byte long
VOLTTYPE_MONEY = 20     # 8 byte long
VOLTTYPE_DECIMAL = 22  # 9 byte long

__NULL = {VOLTTYPE_TINYINT: -128,
          VOLTTYPE_SMALLINT: -32768,
          VOLTTYPE_INTEGER: -2147483648,
          VOLTTYPE_BIGINT: -9223372036854775808,
          VOLTTYPE_FLOAT: -1.7E+308}

def normalize_value(v, type):
    global __NULL
    if type in __NULL and v == __NULL[type]:
        return None
    elif type == VOLTTYPE_FLOAT:
        return round(v, 12)
    elif type == VOLTTYPE_DECIMAL:
        decimal.getcontext().prec = 19
        return decimal.Decimal(v).quantize(decimal.Decimal('1.000000000000'))
    else:
        return v

def normalize_type(v, type):
    if type == VOLTTYPE_TINYINT or type == VOLTTYPE_SMALLINT or type == VOLTTYPE_INTEGER:
        # promote all integer types to bigint to normalize away hsql/volt
        # casting differences in expression evaluation.
        return VOLTTYPE_BIGINT
    else:
        return type


def normalize_values(tuples, columns):
    # 'c' here is a fastserializer.VoltColumn and
    # I assume t is a fastserializer.VoltTable.
    if hasattr(tuples, "__iter__"):
        for i in xrange(len(tuples)):
            if hasattr(tuples[i], "__iter__"):
                normalize_values(tuples[i], columns)
            else:
                tuples[i] = normalize_value(tuples[i], columns[i].type)
                columns[i].type = normalize_type(tuples[i], columns[i].type)

def recursive_sort(l, index, sorted_cols):
    """Recursively sorts the list of lists column by column in place.
    """

    key = lambda x: x[index]

    if not l:
        return
    if not sorted_cols:
        l[:] = sorted(l, cmp=cmp, key=key)
        sorted_cols.append(index)
    if index in sorted_cols:
        return recursive_sort(l, index + 1, sorted_cols)

    begin = 0
    end = 0
    prev = None
    for i in xrange(len(l)):
        if index >= len(l[i]):
            return

        if l[i][sorted_cols[-1]] != prev:
            if prev != None:
                end = i
                l[begin:end] = sorted(l[begin:end], cmp=cmp, key=key)
                tmp = l[begin:end]
                recursive_sort(tmp, index + 1, sorted_cols + [index])
                l[begin:end] = tmp
                begin = end
            prev = l[i][sorted_cols[-1]]

    l[begin:] = sorted(l[begin:], cmp=cmp, key=key)
    tmp = l[begin:]
    recursive_sort(tmp, index + 1, sorted_cols + [index])
    l[begin:] = tmp

def parse_sql(x):
    """Finds if the SQL statement contains ORDER BY command.
    """

    global __EXPR

    result = __EXPR.search(x)
    if result:
        return result.group(1)
    else:
        return None

def normalize(table, sql):
    """Normalizes the result tuples of ORDER BY statements.
    """

    normalize_values(table.tuples, table.columns)

    sort_col = parse_sql(sql)
    index = None
    if sort_col:
        for i in xrange(len(table.columns)):
            if table.columns[i].name.lower() == sort_col.lower():
                index = i
                break
        if index == None:
            return table

    recursive_sort(table.tuples, 0, index and [index] or [])

    return table
