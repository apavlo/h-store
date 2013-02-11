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

import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.api.BenchmarkComponent;
import edu.brown.api.Loader;
import edu.brown.benchmark.tm1.procedures.GetTableCounts;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObservableExceptionHandler;
import edu.brown.utils.EventObserver;

/**
 * Magical TM1 Loader
 * @author zhe
 * @author pavlo
 */
public class TM1Loader extends Loader {
    private static final Logger LOG = Logger.getLogger(TM1Loader.class);
    private static final boolean d = LOG.isDebugEnabled();

    private final long subscriberSize;
    private final boolean blocking = false;

    public static void main(String[] args) {
        BenchmarkComponent.main(TM1Loader.class, args, true);
    }

    public TM1Loader(String[] args) {
        super(args);
        this.subscriberSize = Math.round(TM1Constants.SUBSCRIBER_SIZE * this.getScaleFactor());
    }

    @Override
    public void load() {
        if (d) LOG.debug(String.format("Starting TM1Loader [subscriberSize=%d, scaleFactor=%.2f]",
                         this.subscriberSize, this.getScaleFactor()));

        final Database catalog_db = this.getCatalogContext().database;

        final Thread threads[] =
        { new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_SUBSCRIBER);
                Table catalog_tbl = catalog_db.getTables().get(TM1Constants.TABLENAME_SUBSCRIBER);
                genSubscriber(catalog_tbl);
                if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_SUBSCRIBER);
            }
        }, new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_ACCESS_INFO);
                Table catalog_tbl = catalog_db.getTables().get(TM1Constants.TABLENAME_ACCESS_INFO);
                genAccessInfo(catalog_tbl);
                if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_ACCESS_INFO);
            }
        }, new Thread() {
            public void run() {
                if (d) LOG.debug("Start loading " + TM1Constants.TABLENAME_SPECIAL_FACILITY + " and " + TM1Constants.TABLENAME_CALL_FORWARDING);
                Table catalog_spe = catalog_db.getTables().get(TM1Constants.TABLENAME_SPECIAL_FACILITY);
                Table catalog_cal = catalog_db.getTables().get(TM1Constants.TABLENAME_CALL_FORWARDING);
                genSpeAndCal(catalog_spe, catalog_cal);
                if (d) LOG.debug("Finished loading " + TM1Constants.TABLENAME_SPECIAL_FACILITY + " and " + TM1Constants.TABLENAME_CALL_FORWARDING);
            }
        } };

        final EventObservableExceptionHandler handler = new EventObservableExceptionHandler();
        handler.addObserver(new EventObserver<Pair<Thread,Throwable>>() {
            @Override
            public void update(EventObservable<Pair<Thread, Throwable>> o, Pair<Thread, Throwable> t) {
                for (Thread thread : threads)
                    thread.interrupt();
            }
        });
        
        try {
            for (Thread t : threads) {
                t.setUncaughtExceptionHandler(handler);
                t.start();
                if (this.blocking)
                    t.join();
            } // FOR
            if (!this.blocking) {
                for (Thread t : threads)
                    t.join();
            }
            this.getClientHandle().drain();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (handler.hasError()) {
                throw new RuntimeException("Error while generating table data.", handler.getError());
            }
        }

//        System.err.println("\n" + this.dumpTableCounts());
    }

    public String dumpTableCounts() {
        if (d) LOG.debug("Getting table counts");
        VoltTable results[] = null;
        try {
            results = this.getClientHandle().callProcedure(GetTableCounts.class.getSimpleName()).getResults();
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
            // assert(expected == count) : "Expected " + expected +
            // " tuples for table " + name + " but got back " + count;
        } // WHILE
        return (ret);
    }

    /**
     * Populate Subscriber table per benchmark spec.
     */
    void genSubscriber(Table catalog_tbl) {
        final VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
        Object row[] = new Object[table.getColumnCount()];
        long total = 0;
        for (long s_id = 0; s_id < this.subscriberSize; s_id++) {
            int col = 0;
            row[col++] = s_id;
            row[col++] = TM1Util.padWithZero(s_id);

            // BIT_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 1);
            } // FOR
              // HEX_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 15);
            }
            // BYTE2_##
            for (int j = 0; j < 10; j++) {
                row[col++] = TM1Util.number(0, 255);
            }
            // MSC_LOCATION + VLR_LOCATION
            for (int j = 0; j < 2; j++) {
                row[col++] = TM1Util.number(0, Integer.MAX_VALUE);
            }
            assert col == table.getColumnCount();
            table.addRow(row);
            total++;

            if (table.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d",
                                 TM1Constants.TABLENAME_SUBSCRIBER, total, this.subscriberSize));
                loadVoltTable(TM1Constants.TABLENAME_SUBSCRIBER, table);
                table.clearRowData();
                assert(table.getRowCount() == 0);
            }
        } // FOR
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d",
                             TM1Constants.TABLENAME_SUBSCRIBER, total, this.subscriberSize));
            loadVoltTable(TM1Constants.TABLENAME_SUBSCRIBER, table);
            table.clearRowData();
            assert(table.getRowCount() == 0);
        }
    }

    /**
     * Populate Access_Info table per benchmark spec.
     */
    void genAccessInfo(Table catalog_tbl) {
        final VoltTable table = CatalogUtil.getVoltTable(catalog_tbl);
        int[] arr = { 1, 2, 3, 4 };

        int[] ai_types = TM1Util.subArr(arr, 1, 4);
        long total = 0;
        for (long s_id = 0; s_id < this.subscriberSize; s_id++) {
            for (int ai_type : ai_types) {
                Object row[] = new Object[table.getColumnCount()];
                row[0] = s_id;
                row[1] = ai_type;
                row[2] = TM1Util.number(0, 255);
                row[3] = TM1Util.number(0, 255);
                row[4] = TM1Util.astring(3, 3);
                row[5] = TM1Util.astring(5, 5);
                table.addRow(row);
                total++;
            } // FOR
            if (table.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %6d / %d",
                                 TM1Constants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
                loadVoltTable(TM1Constants.TABLENAME_ACCESS_INFO, table);
                table.clearRowData();
            }
        } // WHILE
        if (table.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %6d / %d",
                             TM1Constants.TABLENAME_ACCESS_INFO, total, ai_types.length * subscriberSize));
            loadVoltTable(TM1Constants.TABLENAME_ACCESS_INFO, table);
            table.clearRowData();
        }
    }

    /**
     * Populate Special_Facility table and CallForwarding table per benchmark
     * spec.
     */
    void genSpeAndCal(Table catalog_spe, Table catalog_cal) {
        VoltTable speTbl = CatalogUtil.getVoltTable(catalog_spe);
        VoltTable calTbl = CatalogUtil.getVoltTable(catalog_cal);

        long speTotal = 0;
        long calTotal = 0;
        int[] arrSpe = { 1, 2, 3, 4 };
        int[] arrCal = { 0, 8, 6 };

        for (long s_id = 0; s_id < this.subscriberSize; s_id++) {
            int[] sf_types = TM1Util.subArr(arrSpe, 1, 4);
            for (int sf_type : sf_types) {
                Object row_spe[] = new Object[speTbl.getColumnCount()];
                row_spe[0] = s_id;
                row_spe[1] = sf_type;
                row_spe[2] = TM1Util.isActive();
                row_spe[3] = TM1Util.number(0, 255);
                row_spe[4] = TM1Util.number(0, 255);
                row_spe[5] = TM1Util.astring(5, 5);
                speTbl.addRow(row_spe);
                speTotal++;

                // now call_forwarding
                int[] start_times = TM1Util.subArr(arrCal, 0, 3);
                for (int start_time : start_times) {
                    Object row_cal[] = new Object[calTbl.getColumnCount()];
                    row_cal[0] = s_id;
                    row_cal[1] = sf_type;
                    row_cal[2] = start_time;
                    row_cal[3] = start_time + TM1Util.number(1, 8);
                    row_cal[4] = TM1Util.nstring(15, 15);
                    calTbl.addRow(row_cal);
                    calTotal++;
                } // FOR
            } // FOR

            if (calTbl.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_CALL_FORWARDING, calTotal));
                loadVoltTable(TM1Constants.TABLENAME_CALL_FORWARDING, calTbl);
                calTbl.clearRowData();
                assert(calTbl.getRowCount() == 0);
            }
            if (speTbl.getRowCount() >= TM1Constants.BATCH_SIZE) {
                if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_SPECIAL_FACILITY, speTotal));
                loadVoltTable(TM1Constants.TABLENAME_SPECIAL_FACILITY, speTbl);
                speTbl.clearRowData();
                assert(speTbl.getRowCount() == 0);
            }
        } // WHILE
        if (calTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_CALL_FORWARDING, calTotal));
            loadVoltTable(TM1Constants.TABLENAME_CALL_FORWARDING, calTbl);
            calTbl.clearRowData();
            assert(calTbl.getRowCount() == 0);
        }
        if (speTbl.getRowCount() > 0) {
            if (d) LOG.debug(String.format("%s: %d", TM1Constants.TABLENAME_SPECIAL_FACILITY, speTotal));
            loadVoltTable(TM1Constants.TABLENAME_SPECIAL_FACILITY, speTbl);
            speTbl.clearRowData();
            assert(speTbl.getRowCount() == 0);
        }
    }
}