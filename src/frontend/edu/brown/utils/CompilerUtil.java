/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
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
package edu.brown.utils;

import org.hsqldb.HSQLInterface;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.compiler.DDLCompiler;
import org.voltdb.compiler.DatabaseEstimates;
import org.voltdb.compiler.StatementCompiler;
import org.voltdb.compiler.VoltCompiler;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.PlanNodeList;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogUtil;

public abstract class CompilerUtil {

    public static AbstractPlanNode compileSQL(final Procedure catalog_proc, String name, String sql) throws Exception {
        VoltCompiler compiler = new VoltCompiler();
        HSQLInterface hsql = HSQLInterface.loadHsqldb();

        Database catalog_db = (Database) catalog_proc.getParent();
        Catalog catalog = catalog_db.getCatalog();
        Statement catalog_stmt = catalog_proc.getStatements().add(name);

        StatementCompiler.compile(compiler, hsql, catalog, catalog_db, new DatabaseEstimates(), catalog_stmt, sql, true);

        // HACK: For now just return the PlanNodeList from the first fragment
        System.err.println("CATALOG_STMT: " + CatalogUtil.debug(catalog_stmt.getFragments()));
        assert (catalog_stmt.getFragments().get(0) != null);
        String serialized = catalog_stmt.getFragments().get(0).getPlannodetree();
        String jsonString = Encoder.hexDecodeToString(serialized);
        PlanNodeList list = null; // FIXME
                                  // (PlanNodeList)PlanNodeTree.fromJSONObject(new
                                  // JSONObject(jsonString), catalog_db);
        return (list.getRootPlanNode());
    }

    /**
     * Generate a new catalog object for the given schema
     * 
     * @param schema_file
     * @return
     * @throws Exception
     */
    public static Catalog compileCatalog(String schema_file) throws Exception {

        HSQLInterface hzsql = HSQLInterface.loadHsqldb();
        String xmlSchema = null;
        hzsql.runDDLFile(schema_file);
        xmlSchema = hzsql.getXMLFromCatalog(true);

        //
        // Setup fake database connection. Pass stuff to database to get catalog
        // objects
        //
        Catalog catalog = new Catalog();
        catalog.execute("add / clusters " + CatalogUtil.DEFAULT_CLUSTER_NAME);
        catalog.execute("add /clusters[" + CatalogUtil.DEFAULT_CLUSTER_NAME + "] databases " + CatalogUtil.DEFAULT_DATABASE_NAME);
        Database catalog_db = catalog.getClusters().get(CatalogUtil.DEFAULT_CLUSTER_NAME).getDatabases().get(CatalogUtil.DEFAULT_DATABASE_NAME);

        VoltCompiler compiler = new VoltCompiler();
        DDLCompiler ddl_compiler = new DDLCompiler(compiler, hzsql);
        ddl_compiler.fillCatalogFromXML(catalog, catalog_db, xmlSchema);
        return (catalog);
    }

}
