/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Original Version:                                                      *
 *  Zhe Zhang (zhe@cs.brown.edu)                                           *
 *                                                                         *
 *  Modifications by:                                                      *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.benchmark.tm1;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.tm1.procedures.GetTableCounts;

public class TM1Loader extends TM1BaseClient {
    private static final Logger LOG = Logger.getLogger(TM1Loader.class);
    private static final boolean d = LOG.isDebugEnabled();
    
    public volatile boolean notDone = true;
    
    /**
     * Keep track of the number of tuples we inserted
     */
    private Map<String, Long> table_counts = new HashMap<String, Long>(); 

    public static void main(String[] args) {
        if (d) LOG.debug("MAIN: " + TM1Loader.class.getName());
        org.voltdb.benchmark.ClientMain.main(TM1Loader.class, args, true);
    }

    public TM1Loader(String[] args) {
        super(args);
        if (d) LOG.debug("CONSTRUCTOR: " + TM1Loader.class.getName());
        
        for (String tableName : TM1Constants.TABLENAMES) {
            this.table_counts.put(tableName, 0l);    
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        return new String[] {};
    }

    @Override
    public void runLoop() {
        if (d) LOG.debug("Starting TM1Loader [subscriberSize=" + subscriberSize + ",scaleFactor=" + scaleFactor + "]");
        Thread threads[] = new Thread[] {
            new Thread() {
                public void run() {
                    if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_SUBSCRIBER);
                    genSubscriber();
                    if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_SUBSCRIBER);
                }
            },
            new Thread() {
                public void run() {
                    if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_ACCESS_INFO);
                    genAccessInfo();
                    if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_ACCESS_INFO);
                }
            },
            new Thread() {
                public void run() {
                    if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_SPECIAL_FACILITY + " and " + TM1Constants.TABLENAME_CALL_FORWARDING);
                    genSpeAndCal();
                    if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_SPECIAL_FACILITY + " and " + TM1Constants.TABLENAME_CALL_FORWARDING);
                }
            }
        };

        try {
            for (Thread t : threads) {
                t.start();
                if (blocking)
                    t.join();
            } // FOR
            if (!blocking) {
                for (Thread t : threads)
                    t.join();
            }
            m_voltClient.drain();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }

        // System.err.println("\n" + this.dumpTableCounts());

        //if (d) LOG.debug("TM1 loader done. ");
    }

    public String dumpTableCounts() {
        if (d) LOG.debug("Getting table counts");
        VoltTable results[] = null;
        try {
            results = m_voltClient.callProcedure(GetTableCounts.class.getSimpleName()).getResults();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
        assert (results.length == 1);

        String ret = "TM1 Table Stats:";
        while (results[0].advanceRow()) {
            String name = results[0].getString(0);
            long count = results[0].getLong(1);
            ret += String.format("\n%-20s %d", name + ":", count);
            
            // assert(this.table_counts.containsKey(name));
            // long expected = this.table_counts.get(name); 
            // assert(expected == count) : "Expected " + expected + " tuples for table " + name + " but got back " + count; 
        } // WHILE
        return (ret);
    }

    /**
     * Define the internal tables that will be populated and sent to VoltDB.
     */
    VoltTable initSubscriberTable() {
        return new VoltTable(
                new VoltTable.ColumnInfo("S_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("SUB_NBR", VoltType.STRING),
                new VoltTable.ColumnInfo("BIT_1", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_2", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_3", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_4", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_5", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_6", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_7", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_8", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_9", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BIT_10", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_1", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_2", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_3", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_4", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_5", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_6", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_7", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_8", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_9", VoltType.TINYINT),
                new VoltTable.ColumnInfo("HEX_10", VoltType.TINYINT),
                new VoltTable.ColumnInfo("BYTE2_1", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_2", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_3", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_4", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_5", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_6", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_7", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_8", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_9", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("BYTE2_10", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("MSC_LOCATION", VoltType.INTEGER),
                new VoltTable.ColumnInfo("VLR_LOCATION", VoltType.INTEGER));
    }

    VoltTable initAccessInfoTable() {
        return new VoltTable(
                new VoltTable.ColumnInfo("S_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("AI_TYPE", VoltType.TINYINT),
                new VoltTable.ColumnInfo("DATA1", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("DATA2", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("DATA3", VoltType.STRING),
                new VoltTable.ColumnInfo("DATA4", VoltType.STRING));
    }

    VoltTable initSpecialFacilityTable() {
        return new VoltTable(
                new VoltTable.ColumnInfo("S_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("SF_TYPE", VoltType.TINYINT),
                new VoltTable.ColumnInfo("IS_ACTIVE", VoltType.TINYINT),
                new VoltTable.ColumnInfo("ERROR_CNTRL", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("DATA_A", VoltType.SMALLINT),
                new VoltTable.ColumnInfo("DATA_B", VoltType.STRING));
    }

    VoltTable initCallForwardingTable() {
        return new VoltTable(
                new VoltTable.ColumnInfo("S_ID", VoltType.INTEGER),
                new VoltTable.ColumnInfo("SF_TYPE", VoltType.TINYINT),
                new VoltTable.ColumnInfo("START_TIME", VoltType.TINYINT),
                new VoltTable.ColumnInfo("END_TIME", VoltType.TINYINT),
                new VoltTable.ColumnInfo("NUMBERX", VoltType.STRING));
    }

    /**
     * Populate Subscriber table per benchmark spec.
     */
    void genSubscriber() {
        long s_id = 0;
        VoltTable table = initSubscriberTable();
        Object row[] = new Object[table.getColumnCount()];

        long total = 0;
        while (s_id++ < subscriberSize) {
            int col = 0;
            row[col++] = new Long(s_id);
            row[col++] = TM1Util.padWithZero((Long) row[0]);
            
            // BIT_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 1).intValue();
            } // FOR
            // HEX_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 15).intValue();
            }
            // BYTE2_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 255).intValue();
            }
            // msc_location + vlr_location
            for (int j = 0; j < 2; j++) {
                row[col++] = TM1Util.number(0, Integer.MAX_VALUE).intValue();
            }
            assert col == table.getColumnCount();
            table.addRow(row);
            total++;
            
            if (table.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d", TM1Constants.TABLENAME_SUBSCRIBER, total, subscriberSize));
                loadTable(TM1Constants.TABLENAME_SUBSCRIBER, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d", TM1Constants.TABLENAME_SUBSCRIBER, total, subscriberSize));
            loadTable(TM1Constants.TABLENAME_SUBSCRIBER, table);
            table.clearRowData();
        }
    }

    /**
     * Populate Access_Info table per benchmark spec.
     */
    void genAccessInfo() {
        int s_id = 0;
        VoltTable table = initAccessInfoTable();
        int[] arr = { 1, 2, 3, 4 };

        int[] ai_types = TM1Util.subArr(arr, 1, 4);
        long total = 0;
        while (s_id++ < subscriberSize) {
            for (int ai_type : ai_types) {
                Object row[] = new Object[table.getColumnCount()];
                row[0] = new Long(s_id);
                row[1] = ai_type;
                row[2] = TM1Util.number(0, 255).intValue();
                row[3] = TM1Util.number(0, 255).intValue();
                row[4] = TM1Util.astring(3, 3);
                row[5] = TM1Util.astring(5, 5);
                table.addRow(row);
                total++;
            } // FOR
            if (table.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d", TM1Constants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
                loadTable(TM1Constants.TABLENAME_ACCESS_INFO, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d", TM1Constants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
            loadTable(TM1Constants.TABLENAME_ACCESS_INFO, table);
            table.clearRowData();
        }
    }

    /**
     * Populate Special_Facility table and CallForwarding table per benchmark
     * spec.
     */
    void genSpeAndCal() {
        int s_id = 0;
        VoltTable speTbl = initSpecialFacilityTable();
        VoltTable calTbl = initCallForwardingTable();
        long speTotal = 0;
        long calTotal = 0;
        int[] arrSpe = { 1, 2, 3, 4 };
        int[] arrCal = { 0, 8, 6 };

        while (s_id++ < subscriberSize) {
            int[] sf_types = TM1Util.subArr(arrSpe, 1, 4);
            for (int sf_type : sf_types) {
                Object row[] = new Object[speTbl.getColumnCount()];
                row[0] = new Long(s_id);
                row[1] = sf_type;
                row[2] = TM1Util.isActive();
                row[3] = TM1Util.number(0, 255).intValue();
                row[4] = TM1Util.number(0, 255).intValue();
                row[5] = TM1Util.astring(5, 5);
                speTbl.addRow(row);
                speTotal++;

                // now call_forwarding
                int[] start_times = TM1Util.subArr(arrCal, 0, 3);
                for (int start_time : start_times) {
                    Object row_cal[] = new Object[calTbl.getColumnCount()];
                    row_cal[0] = row[0];
                    row_cal[1] = row[1];
                    row_cal[2] = start_time;
                    row_cal[3] = start_time + TM1Util.number(1, 8);
                    row_cal[4] = TM1Util.nstring(15, 15);
                    calTbl.addRow(row_cal);
                    calTotal++;
                } // FOR
            } // FOR
            
            if (calTbl.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_CALL_FORWARDING, calTotal));
                loadTable(TM1Constants.TABLENAME_CALL_FORWARDING, calTbl);
                calTbl.clearRowData();
            }
            if (speTbl.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_SPECIAL_FACILITY, speTotal));
                loadTable(TM1Constants.TABLENAME_SPECIAL_FACILITY, speTbl);
                speTbl.clearRowData();
            }
        } // WHILE
        if (calTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_CALL_FORWARDING, calTotal));
            loadTable(TM1Constants.TABLENAME_CALL_FORWARDING, calTbl);
            calTbl.clearRowData();
        }
        if (speTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_SPECIAL_FACILITY, speTotal));
            loadTable(TM1Constants.TABLENAME_SPECIAL_FACILITY, speTbl);
            speTbl.clearRowData();
        }
    }

    private void loadTable(String tablename, VoltTable table) {
        if (d) LOG.debug("Calling LoadMultipartitionTable for '" + tablename + "' with " + table.getRowCount() + " tuples");
        try {
            m_voltClient.callProcedure("@LoadMultipartitionTable", tablename, table);
            this.table_counts.put(tablename, this.table_counts.get(tablename) + table.getRowCount());
        } catch (Exception e) {
            LOG.fatal("Failed to load data for table " + tablename, e);
            System.exit(1);
        }
    }
    
    @Override
    public String getApplicationName() {
        return "TM1 Benchmark";
    }

    @Override
    public String getSubApplicationName() {
        return "Loader";
    }
}