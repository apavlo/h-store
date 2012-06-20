/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.benchmark.markov;

import java.text.DecimalFormat;
import java.util.ArrayList;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;

public abstract class MarkovTables {

    private static final DecimalFormat f = new DecimalFormat("00");

    public static void addIntegerColumns(ArrayList<VoltTable.ColumnInfo> columns, int count, String prefix) {
        for (int ctr = 0; ctr < count; ctr++) {
            String name = prefix + "_IATTR" + f.format(ctr);
            columns.add(new VoltTable.ColumnInfo(name, VoltType.INTEGER));
        }
        return;
    }

    public static void addStringColumns(ArrayList<VoltTable.ColumnInfo> columns, int count, String prefix) {
        for (int ctr = 0; ctr < count; ctr++) {
            String name = prefix + "_SATTR" + f.format(ctr);
            columns.add(new VoltTable.ColumnInfo(name, VoltType.STRING));
        } // FOR
        return;
    }

    public static VoltTable initializeTableA() {
        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        String prefix = "A";

        columns.add(new VoltTable.ColumnInfo(prefix + "_ID", VoltType.BIGINT));
        addStringColumns(columns, 20, prefix);
        addIntegerColumns(columns, 20, prefix);

        VoltTable.ColumnInfo cols[] = new VoltTable.ColumnInfo[columns.size()];
        return (new VoltTable(columns.toArray(cols)));
    }

    public static VoltTable initializeTableB() {
        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        String prefix = "B";

        columns.add(new VoltTable.ColumnInfo(prefix + "_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_A_ID", VoltType.BIGINT));
        addStringColumns(columns, 16, prefix);
        addIntegerColumns(columns, 16, prefix);

        VoltTable.ColumnInfo cols[] = new VoltTable.ColumnInfo[columns.size()];
        return (new VoltTable(columns.toArray(cols)));
    }

    public static VoltTable initializeTableC() {
        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        String prefix = "C";

        columns.add(new VoltTable.ColumnInfo(prefix + "_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_A_ID", VoltType.BIGINT));
        addStringColumns(columns, 16, prefix);
        addIntegerColumns(columns, 16, prefix);

        VoltTable.ColumnInfo cols[] = new VoltTable.ColumnInfo[columns.size()];
        return (new VoltTable(columns.toArray(cols)));
    }

    public static VoltTable initializeTableD() {
        ArrayList<VoltTable.ColumnInfo> columns = new ArrayList<VoltTable.ColumnInfo>();
        String prefix = "D";

        columns.add(new VoltTable.ColumnInfo(prefix + "_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_B_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_B_A_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_C_ID", VoltType.BIGINT));
        columns.add(new VoltTable.ColumnInfo(prefix + "_C_A_ID", VoltType.BIGINT));
        addStringColumns(columns, 16, prefix);
        addIntegerColumns(columns, 16, prefix);
        VoltTable.ColumnInfo cols[] = new VoltTable.ColumnInfo[columns.size()];
        return (new VoltTable(columns.toArray(cols)));
    }
}