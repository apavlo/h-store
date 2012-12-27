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

import java.lang.reflect.Field;
import java.util.SortedMap;
import java.util.TreeMap;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;

import edu.brown.catalog.CatalogKey;
import edu.brown.mappings.AbstractMapping;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class QueryStatistics extends AbstractStatistics<Statement> {

    public enum Members {
        // Number of times executed (per procedure)
        EXECUTE_COUNT_TOTAL, EXECUTE_PROC_COUNT, EXECUTE_COUNT_MIN, EXECUTE_COUNT_MAX, EXECUTE_COUNT_AVG,

        // Parameter Information
        PARAM_HISTOGRAMS,
        // PARAM_PROC_CORELATIONS,
    };

    public Long execute_count_total = 0l;
    public Long execute_proc_count = 0l;
    public Long execute_count_min = null;
    public Long execute_count_max = null;
    public Long execute_count_avg = null;

    public final SortedMap<Integer, ObjectHistogram> param_histograms = new TreeMap<Integer, ObjectHistogram>();

    public final SortedMap<Integer, SortedMap<Integer, AbstractMapping>> param_proc_corelations = new TreeMap<Integer, SortedMap<Integer, AbstractMapping>>();

    /**
     * Constructor
     * 
     * @param catalog_key
     */
    public QueryStatistics(String catalog_key) {
        super(catalog_key);
    }

    /**
     * Constructor
     * 
     * @param catalog_stmt
     */
    public QueryStatistics(Statement catalog_stmt) {
        super(catalog_stmt);
        this.preprocess((Database) catalog_stmt.getParent().getParent());
    }

    @Override
    public Statement getCatalogItem(Database catalog_db) {
        Statement ret = CatalogKey.getFromKey(catalog_db, this.catalog_key, Statement.class);
        return (ret);
    }

    @Override
    public void preprocess(Database catalog_db) {
        if (this.has_preprocessed)
            return;
        final Statement catalog_stmt = this.getCatalogItem(catalog_db);
        for (StmtParameter catalog_stmt_param : catalog_stmt.getParameters()) {
            int stmt_param_idx = catalog_stmt_param.getIndex();
            this.param_histograms.put(stmt_param_idx, new ObjectHistogram());
        } // FOR
        this.has_preprocessed = true;
    }

    @Override
    public void process(Database catalog_db, TransactionTrace xact) throws Exception {
        final Statement catalog_stmt = this.getCatalogItem(catalog_db);
        //
        // For each query, check whether they are going to our table
        // If so, then we need to update our statistics
        //
        int num_params = catalog_stmt.getParameters().size();
        long execute_count = 0;
        LOG.debug("Examining " + xact + " for instances of " + catalog_stmt);
        for (QueryTrace query : xact.getQueries()) {
            if (!query.getCatalogItemName().equals(CatalogKey.getNameFromKey(this.getCatalogKey())))
                continue;
            this.execute_count_total++;
            execute_count++;
            Object params[] = query.getParams();
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    if (this.param_histograms.containsKey(i) && params[i] != null) {
                        this.param_histograms.get(i).put((Comparable<?>) params[i]);
                    }
                } // FOR
            }
        } // FOR
        if (execute_count > 0)
            this.execute_proc_count++;

        if (this.execute_count_min == null || execute_count < this.execute_count_min) {
            this.execute_count_min = execute_count;
        }
        if (this.execute_count_max == null || execute_count > this.execute_count_max) {
            this.execute_count_max = execute_count;
        }

        return;
    }

    @Override
    public void postprocess(Database catalog_db) throws Exception {
        // Query Exec Avg
        this.execute_count_avg = this.execute_count_total / this.execute_proc_count;
    }

    @Override
    public String debug(Database catalog_db) {
        return (this.debug(catalog_db, QueryStatistics.Members.values()));
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        for (Members element : QueryStatistics.Members.values()) {
            try {
                Field field = QueryStatistics.class.getDeclaredField(element.toString().toLowerCase());
                if (element == Members.PARAM_HISTOGRAMS) {
                    stringer.key(element.name()).object();
                    for (Integer idx : this.param_histograms.keySet()) {
                        stringer.key(idx.toString()).object();
                        this.param_histograms.get(idx).toJSON(stringer);
                        stringer.endObject();
                    } // FOR
                    stringer.endObject();

                    // } else if (element == Members.PARAM_PROC_CORELATIONS) {

                } else {
                    stringer.key(element.name()).value(field.get(this));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }

    @Override
    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        this.preprocess(catalog_db);

        for (Members element : QueryStatistics.Members.values()) {
            try {
                Field field = QueryStatistics.class.getDeclaredField(element.toString().toLowerCase());
                if (element == Members.PARAM_HISTOGRAMS) {

                    // } else if (element == Members.PARAM_PROC_CORELATIONS) {

                } else if (object.isNull(element.name())) {
                    field.set(this, null);
                } else {
                    field.set(this, object.getLong(element.name()));
                }
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
    }
}
