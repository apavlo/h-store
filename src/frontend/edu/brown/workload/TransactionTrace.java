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
package edu.brown.workload;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections15.OrderedMap;
import org.apache.commons.collections15.map.LinkedMap;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.VoltType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.StringUtil;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class TransactionTrace extends AbstractTraceElement<Procedure> {
    public enum Members {
        TXN_ID,
        QUERIES
    };
    
    private long txn_id;
    private List<QueryTrace> queries = new ArrayList<QueryTrace>(); 
    private transient LinkedMap<Integer, List<QueryTrace>> query_batches = new LinkedMap<Integer, List<QueryTrace>>();
    
    public TransactionTrace() {
        super();
    }

    private TransactionTrace(long xact_id, String proc_name, Object params[]) {
        super(proc_name, params);
        this.txn_id = xact_id;
    }
    
    public TransactionTrace(long xact_id, Procedure catalog_proc, Object params[]) {
        this(xact_id, catalog_proc.getName(), params);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected TransactionTrace cloneImpl() {
        TransactionTrace clone = new TransactionTrace(this.txn_id, this.catalog_item_name, this.params);
        clone.queries.clear();
        clone.query_batches.clear();
        for (QueryTrace qt : this.queries) {
            QueryTrace clone_q = (QueryTrace)qt.clone();
            clone.addQuery(clone_q);
        } // FOR
        return (clone);
    }
    
    @Override
    public String toString() {
        String ret = String.format("%s[%s:#%d]",
                                   this.getClass().getSimpleName(),
                                   this.catalog_item_name,
                                   this.txn_id);
        if (this.getWeight() > 1) {
            ret += " - Weight:" + this.getWeight();
        }
        return (ret);
    }
    
    /**
     * Change this TransactionTrace's txn id. Should only be used for testing
     * @param txn_id
     */
    public void setTransactionId(long txn_id) {
        this.txn_id = txn_id;
    }

    /**
     * Return the TransactionId for this TransactionTrace
     * @return the xact_id
     */
    public long getTransactionId() {
        return this.txn_id;
    }
    
    @Override
    public Procedure getCatalogItem(Database catalog_db) {
        assert(catalog_db != null);
        return (catalog_db.getProcedures().get(this.catalog_item_name));
    }
    
    /**
     * Set the given list of queries as the queries executed by this transaction
     * This should only be used for testing
     * @param queries
     */
    public void setQueries(Collection<QueryTrace> queries) {
        this.queries.clear();
        this.query_batches.clear();
        for (QueryTrace q : queries) {
            this.addQuery(q);
        } // FOR
    }
    
    public void addQuery(QueryTrace query) {
        this.queries.add(query);
        if (!this.query_batches.containsKey(query.getBatchId())) {
            this.query_batches.put(query.getBatchId(), new ArrayList<QueryTrace>());
        }
        this.query_batches.get((Integer)query.getBatchId()).add(query);
    }
    
    @Override
    public String debug(Database catalog_db) {
        final Procedure catalog_proc = this.getCatalogItem(catalog_db);
        final String thick_line = StringUtil.DOUBLE_LINE;
        final String thin_line  = StringUtil.SINGLE_LINE;
        
        // Header Info
        StringBuilder sb = new StringBuilder();
        sb.append(thick_line)
          .append(catalog_proc.getName().toUpperCase() + " - Txn#" + this.txn_id + "\n")
          .append("Start Time:   " + this.start_timestamp + "\n")
          .append("Stop Time:    " + this.stop_timestamp + "\n")
          .append("Run Time:     " + (this.stop_timestamp != null ? this.stop_timestamp - this.start_timestamp : "???") + "\n")
          .append("Txn Aborted:  " + this.aborted + "\n")
          .append("Weight:       " + this.weight + "\n")
          .append("# of Queries: " + this.queries.size() + "\n")
          .append("# of Batches: " + this.query_batches.size() + "\n");
        
        // Params
        sb.append("Transasction Parameters: [" + this.params.length + "]\n");
        for (int i = 0; i < this.params.length; i++) {
            ProcParameter catalog_param = catalog_proc.getParameters().get(i);
            Object param = this.params[i];
            String type_name = VoltType.get(catalog_param.getType()).name();
            if (ClassUtil.isArray(param)) type_name += "[" + ((Object[])param).length + "]";
            
            sb.append("   [" + i + "] -> ")
              .append(String.format("%-11s ", "(" + type_name + ")"))
              .append(ClassUtil.isArray(param) ? Arrays.toString((Object[])param) : param)
              .append("\n");
        } // FOR
         
        // Queries
        sb.append(thin_line);
        int ctr = 0;
        for (Integer batch_id : this.query_batches.keySet()) {
            sb.append("Batch #" + batch_id + " (" + this.query_batches.get(batch_id).size() + ")\n");
            for (QueryTrace query : this.query_batches.get(batch_id)) {
                sb.append("   [" + (ctr++) + "] " + query.debug(catalog_db) + "\n");
            } // FOR
        } // FOR
        sb.append(thin_line);
        
        return (sb.toString());
    }

    public Map<Statement, Integer> getStatementCounts(Database catalog_db) {
        Map<Statement, Integer> counts = new HashMap<Statement, Integer>();
        Procedure catalog_proc = this.getCatalogItem(catalog_db);
        for (Statement stmt : catalog_proc.getStatements()) {
            counts.put(stmt, 0);
        }
        for (QueryTrace query : this.queries) {
            Statement stmt = query.getCatalogItem(catalog_db);
            assert(stmt != null) : "Invalid query name '" + query.getCatalogItemName() + "' for " + catalog_proc;
            assert(counts.containsKey(stmt)) : "Unexpected " + CatalogUtil.getDisplayName(stmt) + " in " + catalog_proc;
            counts.put(stmt, counts.get(stmt) + 1);
        }
        return (counts);
    }
    
    /**
     * @return the queries
     */
    public List<QueryTrace> getQueries() {
        return this.queries;
    }
    
    public int getQueryCount() {
        return (this.queries.size());
    }
    
    public int getWeightedQueryCount() {
        int ctr = 0;
        for (QueryTrace qt : this.queries) {
            ctr += qt.getWeight();
        }
        return (ctr);
    }
    
    public QueryTrace getQuery(int idx) {
        return (this.queries.get(idx));
    }
    
    /**
     * Returns the number of batches in this transaction
     * @return
     */
    public int getBatchCount() {
        return (this.query_batches.size());
    }
    
    /**
     * Returns an ordered set of query batch ids for this Transaction
     * @return
     */
    public List<Integer> getBatchIds() {
        return (this.query_batches.asList());
    }
    
    /**
     * Return a mapping of batch ids to a list of QueryTrace elements
     * @return
     */
    public OrderedMap<Integer, List<QueryTrace>> getBatches() {
        return (this.query_batches);
    }
    
    /**
     * Return the list of queries in the given batch
     * @param batch_id
     * @return
     */
    public List<QueryTrace> getBatchQueries(int batch_id) {
        return (this.query_batches.getValue(batch_id));
    }
    
    public void toJSONString(JSONStringer stringer, Database catalog_db) throws JSONException {
        super.toJSONString(stringer, catalog_db);
        stringer.key(Members.TXN_ID.name()).value(this.txn_id);

        stringer.key(Members.QUERIES.name()).array();
        for (QueryTrace query : this.queries) {
            stringer.object();
            query.toJSONString(stringer, catalog_db);
            stringer.endObject();
        } // FOR
        stringer.endArray();
    }
    
    @Override
    protected void fromJSONObject(JSONObject object, Database db) throws JSONException {
        super.fromJSONObject(object, db);
        this.txn_id = object.getLong(Members.TXN_ID.name());
        Procedure catalog_proc = (Procedure)db.getProcedures().get(this.catalog_item_name);
        assert(catalog_proc != null) : "Unexpected procedure '" + this.catalog_item_name + "'";
        try {
            super.paramsFromJSONObject(object, catalog_proc.getParameters(), "type");
        } catch (Exception ex) {
            LOG.fatal("Failed to extract procedure params for txn #" + this.txn_id, ex);
            throw new JSONException(ex);
        }
        
        JSONArray jsonQueries = object.getJSONArray(Members.QUERIES.name());
        for (int i = 0; i < jsonQueries.length(); i++) {
            JSONObject jsonQuery = jsonQueries.getJSONObject(i);
            if (jsonQuery.isNull(AbstractTraceElement.Members.NAME.name())) {
                LOG.warn("The catalog name is null for Query #" + i + " in " + this + ". Ignoring...");
                continue;
            }
            try {
                QueryTrace query = QueryTrace.loadFromJSONObject(jsonQuery, catalog_proc);
                this.addQuery(query);
            } catch (JSONException ex) {
                throw new RuntimeException("Failed to load query trace #" + i + " for transaction record on " + this.catalog_item_name + "]", ex);
            }
        } // FOR
    } 
    
    public static TransactionTrace loadFromJSONObject(JSONObject object, Database db) throws JSONException {
        TransactionTrace xact = new TransactionTrace();
        xact.fromJSONObject(object, db);
        return (xact);
    }

} // END CLASS