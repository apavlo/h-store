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

import java.util.*;

import org.apache.log4j.Logger;

import org.json.*;
import org.voltdb.catalog.*;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogKeyOldVersion;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.MemoryEstimator;
import edu.brown.utils.*;
import edu.brown.workload.*;

/**
 * @author pavlo
 */
public class ProcedureStatistics extends AbstractStatistics<Procedure> {
    private static final Logger LOG = Logger.getLogger(ProcedureStatistics.class);

    public enum Members {
        TABLE_TUPLE_COUNTS, TABLE_AVG_TUPLE_SIZES, TABLE_TOTAL_SIZES, TABLE_READONLY, TABLE_QUERYTYPE_COUNTS,

        PROC_COUNTS, PROC_QUERY_COUNTS, PROC_AVG_QUERY_COUNTS, PROC_READONLY, PROC_QUERYTYPE_COUNTS,
        // TODO PROC_PARAM_HISTOGRAMS,

        // TODO QUERY_STATS,
    };

    //
    // Table Information
    //
    public final SortedMap<String, Integer> table_tuple_counts = new TreeMap<String, Integer>();
    public final SortedMap<String, Integer> table_avg_tuple_sizes = new TreeMap<String, Integer>();
    public final SortedMap<String, Integer> table_total_sizes = new TreeMap<String, Integer>();
    public final SortedMap<String, Boolean> table_readonly = new TreeMap<String, Boolean>();
    public final SortedMap<String, SortedMap<QueryType, Integer>> table_querytype_counts = new TreeMap<String, SortedMap<QueryType, Integer>>();

    //
    // Procedure Information
    //
    public Integer proc_counts = 0;
    public Integer proc_query_counts = 0;
    public Integer proc_avg_query_counts = 0;
    public Boolean proc_readonly = true;
    public final SortedMap<QueryType, Integer> proc_querytype_counts = new TreeMap<QueryType, Integer>();

    //
    // ProcParameter => Histogram
    //
    public final SortedMap<Integer, ObjectHistogram<Object>> proc_param_histograms = new TreeMap<Integer, ObjectHistogram<Object>>();

    //
    // Query Statistics
    //
    public final SortedMap<String, QueryStatistics> query_stats = new TreeMap<String, QueryStatistics>();

    /**
     * @param catalog_key
     */
    public ProcedureStatistics(String catalog_key) {
        super(catalog_key);
    }

    public ProcedureStatistics(Procedure catalog_proc) {
        super(catalog_proc);
        this.preprocess((Database) catalog_proc.getParent());
    }

    @Override
    public Procedure getCatalogItem(Database catalog_db) {
        return (CatalogKey.getFromKey(catalog_db, this.catalog_key, Procedure.class));
    }

    /**
     * 
     */
    public void preprocess(Database catalog_db) {
        if (this.has_preprocessed)
            return;
        final Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, this.catalog_key, Procedure.class);

        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            String table_key = CatalogKey.createKey(catalog_tbl);
            this.table_tuple_counts.put(table_key, 0);
            this.table_avg_tuple_sizes.put(table_key, 0);
            this.table_total_sizes.put(table_key, 0);
            this.table_readonly.put(table_key, true);

            this.table_querytype_counts.put(table_key, new TreeMap<QueryType, Integer>());
            for (QueryType type : QueryType.values()) {
                this.table_querytype_counts.get(table_key).put(type, 0);
            } // FOR
        } // FOR

        for (QueryType type : QueryType.values()) {
            this.proc_querytype_counts.put(type, 0);
        } // FOR

        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            String stmt_name = catalog_stmt.getName();
            this.query_stats.put(stmt_name, new QueryStatistics(catalog_stmt));
        } // FOR

        for (ProcParameter catalog_proc_param : catalog_proc.getParameters()) {
            int proc_param_idx = catalog_proc_param.getIndex();
            this.proc_param_histograms.put(proc_param_idx, new ObjectHistogram<Object>());
        } // FOR

        this.has_preprocessed = true;
        return;
    }

    /**
     * @param xact
     * @throws Exception
     */
    @Override
    public void process(Database catalog_db, TransactionTrace xact) throws Exception {
        this.proc_counts += 1;
        final Procedure catalog_proc = CatalogKey.getFromKey(catalog_db, this.catalog_key, Procedure.class);

        for (QueryTrace query : xact.getQueries()) {
            this.process(catalog_db, query);
        } // FOR

        //
        // Analyze procedure input parameters
        //
        for (int i = 0, cnt = this.proc_param_histograms.size(); i < cnt; i++) {
            ProcParameter catalog_proc_param = catalog_proc.getParameters().get(i);
            if (catalog_proc_param.getIsarray()) {
                Object values[] = xact.getParam(i);
                for (Object value : values) {
                    this.proc_param_histograms.get(i).put(value);
                } // FOR
            } else {
                this.proc_param_histograms.get(i).put(xact.getParam(i));
            }
        } // FOR

        for (QueryStatistics query_stat : this.query_stats.values()) {
            query_stat.process(catalog_db, xact);
        } // FOR
    }

    /**
     * @param query
     * @throws Exception
     */
    protected void process(Database catalog_db, QueryTrace query) throws Exception {
        Statement catalog_stmt = query.getCatalogItem(catalog_db);

        QueryType query_type = QueryType.get(catalog_stmt.getQuerytype());
        this.proc_query_counts += 1;

        if (catalog_stmt.getQuerytype() == QueryType.INSERT.getValue() || catalog_stmt.getQuerytype() == QueryType.UPDATE.getValue() || catalog_stmt.getQuerytype() == QueryType.DELETE.getValue()) {
            this.proc_readonly = false;
        }

        //
        // Count Query Types
        //
        this.proc_querytype_counts.put(query_type, this.proc_querytype_counts.get(query_type) + 1);

        //
        // Get the tables used by this query
        //
        Collection<Table> catalog_tbls = CatalogUtil.getReferencedTables(catalog_stmt);
        if (catalog_tbls.isEmpty()) {
            LOG.fatal("Failed to get the target table for " + CatalogUtil.getDisplayName(catalog_stmt));
            System.exit(1);
        }
        for (Table catalog_tbl : catalog_tbls) {
            assert(catalog_tbl != null) : "Unexpected null table catalog object";
            String table_key = CatalogKey.createKey(catalog_tbl);
            assert(table_key != null) : "Unexpected null table key for '" + catalog_tbl + "'";
            SortedMap<QueryType, Integer> cntMap = this.table_querytype_counts.get(table_key);
            if (cntMap == null) {
                cntMap = new TreeMap<QueryType, Integer>();
                this.table_querytype_counts.put(table_key, cntMap);
            }
            Integer cnt = cntMap.get(query_type);
            if (cnt == null) cnt = 0;
            cntMap.put(query_type, cnt + 1);
        } // FOR

        //
        // Now from this point forward we only want to look at INSERTs
        //
        if (query_type != QueryType.INSERT)
            return;
        if (catalog_tbls.size() > 1) {
            LOG.fatal("Found more than one table for " + CatalogUtil.getDisplayName(catalog_stmt) + ": " + catalog_tbls);
            System.exit(1);
        }

        LOG.debug("Looking at " + CatalogUtil.getDisplayName(catalog_stmt));
        try {
            Table catalog_tbl = CollectionUtil.first(catalog_tbls);
            String table_key = CatalogKey.createKey(catalog_tbl);
            Integer bytes = MemoryEstimator.estimateTupleSize(catalog_tbl, catalog_stmt, query.getParams()).intValue();
            this.table_total_sizes.put(table_key, this.table_total_sizes.get(table_key) + bytes);
            this.table_tuple_counts.put(table_key, this.table_tuple_counts.get(table_key) + 1);
        } catch (ArrayIndexOutOfBoundsException ex) {
            // Silently let these pass...
            LOG.debug("Failed to calculate estimated tuple size for " + query, ex);
        } catch (Exception ex) {
            LOG.fatal("Failed to calculate estimated tuple size for " + query, ex);
            LOG.fatal(query.debug(catalog_db));
            System.exit(1);
        }
    }

    @Override
    public void postprocess(Database catalog_db) throws Exception {
        //
        // Clean-up totals
        //
        Integer count = this.proc_counts;
        Integer queries = this.proc_query_counts;
        if (count > 0) {
            this.proc_avg_query_counts = queries / count;
        }

        //
        // Table Information
        //
        for (String table_key : this.table_tuple_counts.keySet()) {
            count = this.table_tuple_counts.get(table_key);
            Integer bytes = this.table_total_sizes.get(table_key);
            if (count > 0)
                this.table_avg_tuple_sizes.put(table_key, bytes / count);

            //
            // Read-only?
            //
            if (this.table_querytype_counts.get(table_key).get(QueryType.INSERT) > 0 || this.table_querytype_counts.get(table_key).get(QueryType.UPDATE) > 0
                    || this.table_querytype_counts.get(table_key).get(QueryType.DELETE) > 0) {
                this.table_readonly.put(table_key, false);
            }
        } // FOR
    }

    public <T extends CatalogType, U> void debug(SortedMap<T, U> map, String title, StringBuilder sb) {
        sb.append(DEBUG_SPACER + title + ":\n");
        for (T catalog_item : map.keySet()) {
            sb.append(DEBUG_SPACER + DEBUG_SPACER);
            sb.append(catalog_item.getName());
            sb.append(":\t");
            sb.append(map.get(catalog_item));
            sb.append("\n");
        } // FOR
        sb.append("\n");
        return;
    }

    @Override
    public String debug(Database catalog_db) {
        return (this.debug(catalog_db, ProcedureStatistics.Members.values()));
    }

    /**
     * 
     */
    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }

    /**
     * 
     */
    public static String toJSONString(Map<Procedure, ProcedureStatistics> stats) throws Exception {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            for (Procedure catalog_proc : stats.keySet()) {
                stringer.key(catalog_proc.getName()).object();
                stats.get(catalog_proc).toJSONString(stringer);
                stringer.endObject();
            } // FOR
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return new JSONObject(stringer.toString()).toString(2);
    }

    /**
     * @param stringer
     * @throws JSONException
     */
    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        this.writeMap(this.table_tuple_counts, Members.TABLE_TUPLE_COUNTS.name(), stringer);
        this.writeMap(this.table_avg_tuple_sizes, Members.TABLE_AVG_TUPLE_SIZES.name(), stringer);
        this.writeMap(this.table_total_sizes, Members.TABLE_TOTAL_SIZES.name(), stringer);
        this.writeMap(this.table_readonly, Members.TABLE_READONLY.name(), stringer);

        stringer.key(Members.TABLE_QUERYTYPE_COUNTS.name()).object();
        for (String table_key : this.table_querytype_counts.keySet()) {
            this.writeMap(this.table_querytype_counts.get(table_key), table_key, stringer);
        }
        stringer.endObject();

        stringer.key(Members.PROC_COUNTS.name()).value(this.proc_counts);
        stringer.key(Members.PROC_QUERY_COUNTS.name()).value(this.proc_query_counts);
        stringer.key(Members.PROC_AVG_QUERY_COUNTS.name()).value(this.proc_avg_query_counts);
        stringer.key(Members.PROC_READONLY.name()).value(this.proc_readonly);
        this.writeMap(this.proc_querytype_counts, Members.PROC_QUERYTYPE_COUNTS.name(), stringer);
    }

    /**
     * @param object
     * @throws JSONException
     */
    @Override
    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        if (LOG.isDebugEnabled())
            LOG.debug("Populating workload statistics from JSON string");
        this.preprocess(catalog_db);

        JSONObject tblQueryObject = object.getJSONObject(Members.TABLE_QUERYTYPE_COUNTS.name());
        Map<String, String> name_xref = new HashMap<String, String>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            JSONException last_error = null;
            String table_keys[] = { CatalogKey.createKey(catalog_tbl), CatalogKeyOldVersion.createKey(catalog_tbl) };
            for (String table_key : table_keys) {
                try {
                    this.readMap(this.table_querytype_counts.get(table_keys[0]),
                                 table_key,
                                 QueryType.getNameMap(),
                                 Integer.class,
                                 tblQueryObject);
                } catch (JSONException ex) {
                    last_error = ex;
                    continue;
                }
                last_error = null;
                name_xref.put(table_key, table_key);
                break;
            } // FOR
            if (last_error != null) {
                // 2013-10-21
                // I decided to switch this to be just a warning instead of 
                // a fatal warning for Nesime. Deal with it.
                LOG.warn("BUSTED: " + StringUtil.join(",", tblQueryObject.keys()));
                // throw last_error;
            }
        } // FOR

        this.readMap(this.table_tuple_counts, Members.TABLE_TUPLE_COUNTS.name(), name_xref, Integer.class, object);
        this.readMap(this.table_avg_tuple_sizes, Members.TABLE_AVG_TUPLE_SIZES.name(), name_xref, Integer.class, object);
        this.readMap(this.table_total_sizes, Members.TABLE_TOTAL_SIZES.name(), name_xref, Integer.class, object);
        this.readMap(this.table_readonly, Members.TABLE_READONLY.name(), name_xref, Integer.class, object);

        this.proc_counts = object.getInt(Members.PROC_COUNTS.name());
        this.proc_query_counts = object.getInt(Members.PROC_QUERY_COUNTS.name());
        this.proc_avg_query_counts = object.getInt(Members.PROC_AVG_QUERY_COUNTS.name());
        this.proc_readonly = object.getBoolean(Members.PROC_READONLY.name());
        this.readMap(this.proc_querytype_counts, Members.PROC_QUERYTYPE_COUNTS.name(), QueryType.getNameMap(), Integer.class, object);
    }
}
