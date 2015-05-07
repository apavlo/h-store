/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
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
package edu.brown.statistics;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.CatalogKey.InvalidCatalogKey;
import edu.brown.utils.*;
import edu.brown.workload.*;

/**
 * @author pavlo
 */
public class WorkloadStatistics implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(WorkloadStatistics.class.getName());

    public enum Members {
        PROC_STATS, TABLE_STATS,
    };

    public final Map<String, ProcedureStatistics> proc_stats = new HashMap<String, ProcedureStatistics>();
    public final Map<String, TableStatistics> table_stats = new HashMap<String, TableStatistics>();

    /**
     * Constructor
     * 
     * @param workload
     * @param catalog_db
     */
    public WorkloadStatistics(Database catalog_db) {
        this.init(catalog_db);
    }

    /**
     * Stored the given TableStatistics in this object. No questions asked!
     * 
     * @param table_stats
     */
    public void apply(Map<Table, TableStatistics> table_stats) {
        for (Entry<Table, TableStatistics> e : table_stats.entrySet()) {
            this.table_stats.put(CatalogKey.createKey(e.getKey()), e.getValue());
        } // FOR
    }

    public Collection<TableStatistics> getTableStatistics() {
        return (this.table_stats.values());
    }

    public TableStatistics getTableStatistics(Table catalog_tbl) {
        return (this.table_stats.get(CatalogKey.createKey(catalog_tbl)));
    }

    public TableStatistics getTableStatistics(String table_key) {
        return (this.table_stats.get(table_key));
    }

    public void addTableStatistics(Table catalog_tbl, TableStatistics stats) {
        TableStatistics orig = this.table_stats.put(CatalogKey.createKey(catalog_tbl), stats);
        assert (orig == null) : "Duplicate TableStatistics for " + catalog_tbl;
    }

    public Collection<ProcedureStatistics> getProcedureStatistics() {
        return (this.proc_stats.values());
    }

    public ProcedureStatistics getProcedureStatistics(Procedure catalog_proc) {
        return (this.proc_stats.get(CatalogKey.createKey(catalog_proc)));
    }

    public ProcedureStatistics getProcedureStatistics(String proc_key) {
        return (this.proc_stats.get(proc_key));
    }

    /**
     * 
     */
    protected void init(Database catalog_db) {
        // Procedure Stats
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            this.proc_stats.put(CatalogKey.createKey(catalog_proc), new ProcedureStatistics(catalog_proc));
        } // FOR

        // Table Stats
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            this.table_stats.put(CatalogKey.createKey(catalog_tbl), new TableStatistics(catalog_tbl));
        } // FOR
    }

    public String debug(Database catalog_db) {
        StringBuilder buffer = new StringBuilder();
        boolean first = true;
        for (ProcedureStatistics proc_stat : this.proc_stats.values()) {
            if (first == false)
                buffer.append("-----------\n");
            buffer.append(proc_stat.debug(catalog_db));
            for (QueryStatistics query_stat : proc_stat.query_stats.values()) {
                if (query_stat.execute_count_total == 0)
                    continue;
                buffer.append("\n").append(query_stat.debug(catalog_db));
            } // FOR
            first = false;
        } // FOR
        buffer.append("\n+-------------------------------------------------\n");
        for (TableStatistics table_stat : this.table_stats.values()) {
            buffer.append(table_stat.debug(catalog_db)).append("\n");
        }
        return (buffer.toString());
    }

    /**
     * @param workload
     * @throws Exception
     */
    public void process(final Database catalog_db, final Workload workload) throws Exception {
        // -----------------------------------------------------
        // PRE-PROCESS
        // -----------------------------------------------------
        LOG.info("Invoking preprocess methods on statistics objects for " + workload);

        // Procedure Stats
        for (ProcedureStatistics proc_stat : this.proc_stats.values()) {
            proc_stat.preprocess(catalog_db);
        } // FOR

        // Table Stats
        for (TableStatistics table_stat : this.table_stats.values()) {
            table_stat.preprocess(catalog_db);
        } // FOR

        // -----------------------------------------------------
        // PROCESS
        // -----------------------------------------------------
        LOG.info("Invoking process methods on statistics objects for " + workload);
        // List<Thread> threads = new ArrayList<Thread>();
        // int num_threads = 10;
        // long max_id = workload.getMaxTraceId();
        // long fraction = (max_id / num_threads);

        final AtomicInteger count = new AtomicInteger(0);
        // for (int i = 0; i < num_threads; i++) {
        // final long start_id = i * fraction;
        // final long stop_id = (i + 1) * fraction;
        //
        // threads.add(new Thread() {
        // public void run() {
        for (TransactionTrace element : workload) {
            // if (element.getId() < start_id) continue;
            // if (element.getId() > stop_id) break;

            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace) element;
                Procedure catalog_proc = xact.getCatalogItem(catalog_db);
                if (catalog_proc.getSystemproc())
                    continue;

                // Procedure Stats
                try {
                    String proc_key = CatalogKey.createKey(catalog_proc);
                    synchronized (catalog_proc) {
                        WorkloadStatistics.this.proc_stats.get(proc_key).process(catalog_db, xact);
                    } // SYNCHRONIZED

                    // Table Stats
                    for (Table catalog_tbl : CatalogUtil.getReferencedTables(catalog_proc)) {
                        String table_key = CatalogKey.createKey(catalog_tbl);
                        synchronized (catalog_tbl) {
                            TableStatistics tableStats = WorkloadStatistics.this.table_stats.get(table_key);
                            if (tableStats == null) {
                                tableStats = new TableStatistics(table_key);
                                WorkloadStatistics.this.table_stats.put(table_key, tableStats);
                            }
                            tableStats.process(catalog_db, xact);
                        } // SYNCHRONIZED
                    } // FOR
                } catch (Exception ex) {
                    LOG.error("Failed to process " + xact, ex);
                    System.exit(1);
                }
                if (count.getAndIncrement() % 1000 == 0)
                    LOG.debug("Processed " + count.get() + " transaction traces");
            }
        } // FOR
        // }
        // });
        // } // FOR
        // ThreadUtil.runNewPool(threads);

        // -----------------------------------------------------
        // POST-PROCESS
        // -----------------------------------------------------
        LOG.info("Invoking postprocess methods on statistics objects for " + workload);

        // Procedure Stats
        for (ProcedureStatistics proc_stat : this.proc_stats.values()) {
            proc_stat.postprocess(catalog_db);
        } // FOR

        // Table Stats
        for (TableStatistics table_stat : this.table_stats.values()) {
            table_stat.postprocess(catalog_db);
        } // FOR
    }

    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // Procedure Statistics
        stringer.key(Members.PROC_STATS.name()).object();
        for (String proc_key : this.proc_stats.keySet()) {
            stringer.key(proc_key).object();
            this.proc_stats.get(proc_key).toJSONString(stringer);
            stringer.endObject();
        } // FOR
        stringer.endObject();

        // Table Statistics
        stringer.key(Members.TABLE_STATS.name()).object();
        for (String tbl_key : this.table_stats.keySet()) {
            stringer.key(tbl_key).object();
            this.table_stats.get(tbl_key).toJSONString(stringer);
            stringer.endObject();
        } // FOR
        stringer.endObject();
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // Procedure Statistics
        JSONObject jsonProcStats = json_object.getJSONObject(Members.PROC_STATS.name());
        Iterator<String> keys = jsonProcStats.keys();
        while (keys.hasNext()) {
            String proc_key = keys.next();
            Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, proc_key, Procedure.class);
            if (catalog_proc == null) {
                throw new JSONException("Invalid procedure name '" + proc_key + "'");
            }
            ProcedureStatistics proc_stat = new ProcedureStatistics(catalog_proc);
            proc_stat.fromJSONObject(jsonProcStats.getJSONObject(proc_key), catalog_db);
            this.proc_stats.put(CatalogKey.createKey(catalog_proc), proc_stat);
        } // WHILE

        // Table Statistics
        JSONObject jsonTableStats = json_object.getJSONObject(Members.TABLE_STATS.name());
        for (String table_key : CollectionUtil.iterable(jsonTableStats.keys())) {
            // Ignore any missing tables
            Table catalog_tbl = null;
            try {
                catalog_tbl = CatalogKey.getFromKey(catalog_db, table_key, Table.class);
            } catch (InvalidCatalogKey ex) {
                LOG.warn("Ignoring invalid table '" + CatalogKey.getNameFromKey(table_key));
                continue;
            }
            if (catalog_tbl == null) {
                throw new JSONException("Invalid table catalog key '" + table_key + "'");
            }
            if (catalog_tbl.getSystable())
                continue;
            TableStatistics table_stat = new TableStatistics(catalog_tbl);
            table_stat.fromJSONObject(jsonTableStats.getJSONObject(table_key), catalog_db);
            this.table_stats.put(CatalogKey.createKey(catalog_tbl), table_stat);
        } // WHILE
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG
        // ArgumentsParser.PARAM_WORKLOAD,
        // ArgumentsParser.PARAM_STATS_OUTPUT
        );

        // We can be passed in stats that these new stats will get appended to
        // it
        if (args.stats == null)
            args.stats = new WorkloadStatistics(args.catalog_db);

        LOG.info("Workload Histogram:\n" + args.workload.getProcedureHistogram());

        // Set all the tables to read-only (why???)
        for (TableStatistics table_stats : args.stats.getTableStatistics()) {
            table_stats.readonly = true;
        }
        args.stats.process(args.catalog_db, args.workload);
        if (args.getParam(ArgumentsParser.PARAM_STATS_OUTPUT) != null) {
            args.stats.save(args.getFileParam(ArgumentsParser.PARAM_STATS_OUTPUT));
        }
        // System.out.println(args.stats.debug(args.catalog_db));

        // for (TableStatistics table_stat : args.stats.table_stats.values()) {
        // System.out.println(table_stat.debug(args.catalog_db));
        // System.out.println("---------------------------------------------------------------");
        // } // FOR
        //
        // for (ProcedureStatistics proc_stat : args.stats.proc_stats.values())
        // {
        // for (QueryStatistics query_stat : proc_stat.query_stats.values()) {
        // if (query_stat.execute_count_total == 0) continue;
        // System.out.println(query_stat.debug(args.catalog_db));
        // System.out.println("---------------------------------------------------------------");
        // } // FOR
        // } // FOR
    }

}
