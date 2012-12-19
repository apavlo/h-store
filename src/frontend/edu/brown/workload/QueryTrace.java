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

import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.ClassUtil;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class QueryTrace extends AbstractTraceElement<Statement> {
    public enum Members {
        BATCH_ID,
    };
    
    private int batch_id;
    
    public QueryTrace() {
        super();
    }
    
    public QueryTrace(String catalog_stmt_name, Object params[], int batch_id) {
        super(catalog_stmt_name, params);
        this.batch_id = batch_id;
    }
    
    public QueryTrace(Statement catalog_statement, Object params[], int batch_id) {
        this(CatalogKey.createKey(catalog_statement), params, batch_id);
    }
    
    @SuppressWarnings("unchecked")
    @Override
    protected QueryTrace cloneImpl() {
        QueryTrace clone = new QueryTrace(this.catalog_item_name, this.params, this.batch_id);
        return (clone);
    }
    
    @Override
    public String getCatalogItemName() {
        return CatalogKey.getNameFromKey(super.getCatalogItemName());
    }
    
    /**
     * 
     * @return
     */
    public int getBatchId() {
        return this.batch_id;
    }
    
    @Override
    public Statement getCatalogItem(Database catalog_db) {
        return (CatalogKey.getFromKey(catalog_db, this.catalog_item_name, Statement.class));
    }
    
    /**
     * Grabs the parent procedure for this query/statement
     * @return the Procedure catalog object
     */
    public Procedure getCatalogProcedure(Database catalog_db) {
        Statement catalog_stmt = this.getCatalogItem(catalog_db);
        return catalog_stmt.getParent();
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "[" + this.catalog_item_name + "]");
    }
    
    @Override
    public String debug(Database catalog_db) {
        String ret = this.catalog_item_name;
        
        // Params
        ret += " - Parameters: [";
        String add = "";
        for (Object param : this.params) {
            ret += add + (ClassUtil.isArray(param) ? Arrays.toString((Object[])param) : param);
            add = ", ";
        } // FOR
        ret += "]";
        if (this.weight > 1) ret += String.format(" - Weight: %d", this.weight);
        if (this.aborted) ret += " *ABORTED*";
        
        return (ret);
    }
        
    @Override
    public void toJSONString(JSONStringer stringer, Database catalog_db) throws JSONException {
        super.toJSONString(stringer, catalog_db);
        stringer.key(Members.BATCH_ID.name()).value(this.batch_id);
    }
    
    @Override
    protected void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        throw new RuntimeException("Unimplemented!");
    }
    
    protected void fromJSONObject(JSONObject object, Procedure catalog_proc) throws JSONException {
        super.fromJSONObject(object, CatalogUtil.getDatabase(catalog_proc));
        this.batch_id = object.getInt(Members.BATCH_ID.name());
        
        Statement catalog_stmt = null;
        if (this.catalog_item_name.contains(":") == false) {
            catalog_stmt = catalog_proc.getStatements().getIgnoreCase(this.catalog_item_name);
        } else {
            catalog_stmt = CatalogKey.getFromKey(CatalogUtil.getDatabase(catalog_proc),
                                                 this.catalog_item_name, Statement.class);
        }
        if (catalog_stmt == null) {
            catalog_stmt = catalog_proc.getStatements().getIgnoreCase(CatalogKey.getNameFromKey(this.catalog_item_name));
            if (catalog_stmt == null) {
                String msg = "Procedure '" + catalog_proc.getName() + "' does not have a Statement '" + this.catalog_item_name + "'";
                msg += "\nValid Statements: " + CatalogUtil.debug(catalog_proc.getStatements());
                throw new JSONException(msg);
            }
        }
        // HACK
        this.catalog_item_name = CatalogKey.createKey(catalog_stmt);
        
        try {
            super.paramsFromJSONObject(object, catalog_stmt.getParameters(), "javatype");
        } catch (Exception ex) {
            LOG.fatal("Failed to load query trace for " + this.catalog_item_name);
            throw new JSONException(ex);
        }
    } 
    
    public static QueryTrace loadFromJSONObject(JSONObject object, Procedure catalog_proc) throws JSONException {
        QueryTrace query = new QueryTrace();
        query.fromJSONObject(object, catalog_proc);
        return (query);
    }
} // END CLASS