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

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.MemoryEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class TableStatistics extends AbstractStatistics<Table> {

    public enum Members {
        // Whether this table was read-only in the workload
        READONLY,

        // Estimate number of tuples (based on the number of inserts)
        TUPLE_COUNT_TOTAL,

        // Estimate tuple sizes
        TUPLE_SIZE_TOTAL, TUPLE_SIZE_MIN, TUPLE_SIZE_MAX, TUPLE_SIZE_AVG,

        // The type of queries used on this table
        QUERY_TYPE_COUNT,

        // Column statistics
        COLUMN_STATS,
    };

    /** Total number of tuples in the table */
    public Long tuple_count_total = 0l;
    /** Total number of bytes for the table */
    public Long tuple_size_total = 0l;
    /** The minimum size of a tupe in the table */
    public Long tuple_size_min = null;
    /** The maximum size of a tuple in the table */
    public Long tuple_size_max = null;
    /** The average size of a tuple in the table */
    public Long tuple_size_avg = 0l;
    /** Is this table readonly */
    public Boolean readonly = true;

    public final SortedMap<QueryType, Long> query_type_count = new TreeMap<QueryType, Long>();
    public final SortedMap<String, ColumnStatistics> column_stats = new TreeMap<String, ColumnStatistics>();

    /**
     * Options
     */
    protected boolean estimateSize = true;
    protected boolean estimateCount = false;

    //
    // The field types we will record
    //
    public static final Set<VoltType> TARGET_COLUMN_TYPES = new HashSet<VoltType>();
    static {
        TARGET_COLUMN_TYPES.add(VoltType.TINYINT);
        TARGET_COLUMN_TYPES.add(VoltType.INTEGER);
        TARGET_COLUMN_TYPES.add(VoltType.BIGINT);
    };

    private final transient Set<ColumnStatistics> target_columns = new HashSet<ColumnStatistics>();

    /**
     * Construtor
     * 
     * @param catalog_key
     */
    public TableStatistics(String catalog_key) {
        super(catalog_key);
    }

    public TableStatistics(Table catalog_tbl) {
        super(catalog_tbl);
        this.preprocess((Database) catalog_tbl.getParent());
    }

    @Override
    public Table getCatalogItem(Database catalog_db) {
        Table ret = CatalogKey.getFromKey(catalog_db, this.catalog_key, Table.class);
        if (ret == null) {
            System.err.println("catalog_key: " + this.catalog_key);
            System.err.println("tables:      " + CatalogUtil.debug(catalog_db.getTables()));
        }
        return (ret);
    }

    public Collection<ColumnStatistics> getColumnStatistics() {
        return (this.column_stats.values());
    }

    public ColumnStatistics getColumnStatistics(Column catalog_col) {
        String col_key = CatalogKey.createKey(catalog_col);
        return (this.getColumnStatistics(col_key));
    }

    public ColumnStatistics getColumnStatistics(String col_key) {
        return (this.column_stats.get(col_key));
    }

    public void process(Database catalog_db, VoltTableRow row) {
        long rowSize = row.getRowSize();
        this.tuple_count_total++;
        this.tuple_size_total += rowSize;

        if (this.tuple_size_min == null || this.tuple_size_min > rowSize) {
            this.tuple_size_min = rowSize;
        }
        if (this.tuple_size_max == null || this.tuple_size_max > rowSize) {
            this.tuple_size_max = rowSize;
        }

        // XXX: Should we call call process for the ColumnStats?
    }

    @Override
    public void preprocess(Database catalog_db) {
        if (this.has_preprocessed)
            return;

        Table catalog_tbl = this.getCatalogItem(catalog_db);

        for (QueryType type : QueryType.values()) {
            this.query_type_count.put(type, 0l);
        } // FOR
        for (Column catalog_col : catalog_tbl.getColumns()) {
            String col_key = CatalogKey.createKey(catalog_col);
            this.column_stats.put(col_key, new ColumnStatistics(catalog_col));
        } // FOR

        this.has_preprocessed = true;
    }

    @Override
    public void process(Database catalog_db, TransactionTrace xact) throws Exception {
        final Table catalog_tbl = this.getCatalogItem(catalog_db);
        // For each query, check whether they are going to our table
        // If so, then we need to update our statistics
        for (QueryTrace query : xact.getQueries()) {
            Statement catalog_stmt = query.getCatalogItem(catalog_db);
            QueryType query_type = QueryType.get(catalog_stmt.getQuerytype());
            // System.out.println("Examining " + catalog_stmt + " for " +
            // catalog_tbl);

            if (CatalogUtil.getReferencedTables(catalog_stmt).contains(catalog_tbl)) {
                // Query Type Counts
                Long cnt = this.query_type_count.get(query_type);
                if (cnt == null) {
                    cnt = 0l;
                }
                this.query_type_count.put(query_type, cnt + 1);

                // Read-only
                if (query_type == QueryType.INSERT || query_type == QueryType.UPDATE || query_type == QueryType.DELETE) {
                    this.readonly = false;
                }

                // Now from this point forward we only want to look at INSERTs
                // because
                // that's the only way we can estimate tuple sizes
                if (query_type == QueryType.INSERT) {
                    this.tuple_count_total += 1;
                    Long bytes = 0l;
                    try {
                        bytes = (long) MemoryEstimator.estimateTupleSize(catalog_tbl, catalog_stmt, query.getParams()).intValue();
                    } catch (ArrayIndexOutOfBoundsException ex) {
                        // Silently let these pass...
                        LOG.debug("Failed to calculate estimated tuple size for " + query, ex);
                    }
                    this.tuple_size_total += bytes;
                    if (bytes != 0 && (this.tuple_size_min == null || this.tuple_size_min > bytes)) {
                        this.tuple_size_min = bytes;
                    }
                    if (this.tuple_size_max == null || this.tuple_size_max < bytes) {
                        this.tuple_size_max = bytes;
                    }
                }

                // Column Stats
                if (query_type == QueryType.INSERT) {
                    if (this.target_columns.isEmpty()) {
                        for (Column catalog_col : catalog_tbl.getColumns()) {
                            VoltType col_type = VoltType.get((byte) catalog_col.getType());
                            if (TARGET_COLUMN_TYPES.contains(col_type)) {
                                String col_key = CatalogKey.createKey(catalog_col);
                                this.target_columns.add(this.column_stats.get(col_key));
                            }
                        } // FOR
                    }
                    for (ColumnStatistics col_stats : this.target_columns) {
                        col_stats.process(catalog_db, xact);
                    } // FOR
                }
            } // IF
        } // FOR
        return;
    }

    @Override
    public void postprocess(Database catalog_db) throws Exception {
        if (this.tuple_count_total > 0) {
            this.tuple_size_avg = this.tuple_size_total / this.tuple_count_total;
        }
    }

    @Override
    public String debug(Database catalog_db) {
        return (this.debug(catalog_db, TableStatistics.Members.values()));
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        // Tuple Counts
        stringer.key(Members.TUPLE_COUNT_TOTAL.name()).value(this.tuple_count_total);

        // Tuple Sizes
        stringer.key(Members.TUPLE_SIZE_TOTAL.name()).value(this.tuple_size_total);
        stringer.key(Members.TUPLE_SIZE_MIN.name()).value(this.tuple_size_min);
        stringer.key(Members.TUPLE_SIZE_MAX.name()).value(this.tuple_size_max);
        stringer.key(Members.TUPLE_SIZE_AVG.name()).value(this.tuple_size_avg);

        // Read-only
        stringer.key(Members.READONLY.name()).value(this.readonly);

        // Query Type Counts
        this.writeMap(this.query_type_count, Members.QUERY_TYPE_COUNT.name(), stringer);

        // Column Stats
        stringer.key(Members.COLUMN_STATS.name()).object();
        for (String col_key : this.column_stats.keySet()) {
            stringer.key(col_key).object();
            this.column_stats.get(col_key).toJSONString(stringer);
            stringer.endObject();
        } // FOR
        stringer.endObject();
    }

    @Override
    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        this.preprocess(catalog_db);

        // Tuple Counts
        this.tuple_count_total = object.getLong(Members.TUPLE_COUNT_TOTAL.name());

        // Tuple Sizes
        this.tuple_size_total = object.getLong(Members.TUPLE_SIZE_TOTAL.name());

        if (!object.isNull(Members.TUPLE_SIZE_MIN.name())) {
            this.tuple_size_min = object.getLong(Members.TUPLE_SIZE_MIN.name());
        }
        if (!object.isNull(Members.TUPLE_SIZE_MAX.name())) {
            this.tuple_size_max = object.getLong(Members.TUPLE_SIZE_MAX.name());
        }
        this.tuple_size_avg = object.getLong(Members.TUPLE_SIZE_AVG.name());

        // Read-only
        this.readonly = object.getBoolean(Members.READONLY.name());

        // Query Type Counts
        this.readMap(this.query_type_count, Members.QUERY_TYPE_COUNT.name(), QueryType.getNameMap(), Long.class, object);

        // Column Stats
        if (object.has(Members.COLUMN_STATS.name())) {
            JSONObject jsonObject = object.getJSONObject(Members.COLUMN_STATS.name());
            Iterator<String> col_keys = jsonObject.keys();
            while (col_keys.hasNext()) {
                String col_key = col_keys.next();
                Column catalog_col = CatalogKey.getFromKey(catalog_db, col_key, Column.class);
                ColumnStatistics col_stats = new ColumnStatistics(catalog_col);
                col_stats.fromJSONObject(jsonObject.getJSONObject(col_key), catalog_db);
                this.column_stats.put(col_key, col_stats);
            } // WHILE
        }
    }
}