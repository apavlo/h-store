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

import org.json.*;
import org.voltdb.catalog.*;

import edu.brown.utils.ClassUtil;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class QueryTrace extends AbstractTraceElement<Statement> {
    public enum Members {
        BATCH_ID,
        PROC_NAME,
    };
    
    protected int batch_id;
    protected String proc_name;
    
    public QueryTrace() {
        super();
    }
    
    public QueryTrace(String xact_id, Statement catalog_statement, Object params[], int batch_id) {
        super(xact_id, catalog_statement, params);
        this.batch_id = batch_id;
        this.proc_name = ((Procedure)catalog_statement.getParent()).getName();
    }
    
    /**
     * 
     * @return
     */
    public int getBatchId() {
        return this.batch_id;
    }

    /**
     * Returns the name of the parent Procedure for this query
     * @return
     */
    public String getProcedureName() {
        return proc_name;
    }
    
    public String getFullName() {
        return (this.proc_name + "." + this.catalog_item_name);
    }
    
    
    @Override
    public Statement getCatalogItem(Database catalog_db) {
        return (this.getCatalogProcedure(catalog_db).getStatements().get(this.catalog_item_name));
    }
    
    /**
     * Grabs the parent procedure for this query/statement
     * @return the Procedure catalog object
     */
    public Procedure getCatalogProcedure(Database catalog_db) {
        return catalog_db.getProcedures().get(this.proc_name);
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "[" + this.proc_name + "." + this.catalog_item_name + ":" + this.id + "]");
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
        
        return (ret);
    }
        
    @Override
    public void toJSONString(JSONStringer stringer, Database catalog_db) throws JSONException {
        super.toJSONString(stringer, catalog_db);
        stringer.key(Members.BATCH_ID.name()).value(this.batch_id);
        stringer.key(Members.PROC_NAME.name()).value(this.proc_name);
    }
    
    @Override
    protected void fromJSONObject(JSONObject object, Database db) throws JSONException {
        super.fromJSONObject(object, db);
        this.proc_name = object.getString(Members.PROC_NAME.name());
        this.batch_id = object.getInt(Members.BATCH_ID.name());
        
        Statement catalog_stmt = db.getProcedures().get(this.proc_name).getStatements().get(this.catalog_item_name);
        if (catalog_stmt == null)
            throw new JSONException("Procedure '" + this.proc_name + "' does not have a Statement '" + this.catalog_item_name + "'");
        
        try {
            super.paramsFromJSONObject(object, catalog_stmt.getParameters(), "javatype");
        } catch (Exception ex) {
            LOG.fatal("Failed to load query trace for " + this.catalog_item_name);
            throw new JSONException(ex);
        }
    } 
    
    public static QueryTrace loadFromJSONObject(JSONObject object, Database db) throws JSONException {
        QueryTrace query = new QueryTrace();
        query.fromJSONObject(object, db);
        return (query);
    }
} // END CLASS