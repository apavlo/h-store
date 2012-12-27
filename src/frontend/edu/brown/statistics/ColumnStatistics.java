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

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class ColumnStatistics extends AbstractStatistics<Column> {

    public enum Members {
        READONLY, HISTOGRAM,
    };

    public Boolean readonly = true;
    public ObjectHistogram<Object> histogram = new ObjectHistogram<Object>();

    public ColumnStatistics(String catalog_key) {
        super(catalog_key);
    }

    public ColumnStatistics(Column catalog_col) {
        super(catalog_col);
        this.preprocess((Database) catalog_col.getParent().getParent());
    }

    @Override
    public Column getCatalogItem(Database catalog_db) {
        Column ret = CatalogKey.getFromKey(catalog_db, this.catalog_key, Column.class);
        return (ret);
    }

    @Override
    public void preprocess(Database catalog_db) {
        if (this.has_preprocessed)
            return;

        // Do nothing...

        this.has_preprocessed = true;
    }

    @Override
    public void process(Database catalog_db, TransactionTrace xact) throws Exception {
        final Column catalog_col = this.getCatalogItem(catalog_db);
        final Table catalog_tbl = (Table) catalog_col.getParent();
        // For each query, check whether they are going to our table
        // If so, then we need to update our statistics
        int col_index = catalog_col.getIndex();
        for (QueryTrace query : xact.getQueries()) {
            Statement catalog_stmt = query.getCatalogItem(catalog_db);
            QueryType query_type = QueryType.get(catalog_stmt.getQuerytype());
            // For now we only examine the tuples as they are being inserted
            // into the table
            if (query_type == QueryType.INSERT && CatalogUtil.getReferencedTables(catalog_stmt).contains(catalog_tbl)) {
                try {
                    Object value = query.getParam(col_index);
                    this.histogram.put(value);
                } catch (ArrayIndexOutOfBoundsException ex) {
                    // Silently let these pass...
                    LOG.debug(query + " [col_index=" + col_index + "]", ex);
                }
            }
        } // FOR
        return;
    }

    @Override
    public void postprocess(Database catalog_db) throws Exception {
        // Do nothing...
    }

    @Override
    public String debug(Database catalog_db) {
        return (this.debug(catalog_db, ColumnStatistics.Members.values()));
    }

    @Override
    public void toJSONString(JSONStringer stringer) throws JSONException {
        // Read-only
        stringer.key(Members.READONLY.name()).value(this.readonly);

        // Histogram
        stringer.key(Members.HISTOGRAM.name()).object();
        this.histogram.toJSON(stringer);
        stringer.endObject();
    }

    @Override
    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        this.preprocess(catalog_db);

        // Read-only
        this.readonly = object.getBoolean(Members.READONLY.name());

        // Histogram
        this.histogram.fromJSON(object.getJSONObject(Members.HISTOGRAM.name()), catalog_db);
    }
}
