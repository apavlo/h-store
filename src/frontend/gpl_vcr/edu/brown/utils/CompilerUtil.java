package edu.brown.utils;

import org.hsqldb.HSQLInterface;
import org.json.*;

import org.voltdb.BackendTarget;
import org.voltdb.compiler.*;
import org.voltdb.catalog.*;
import org.voltdb.plannodes.*;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogUtil;

public abstract class CompilerUtil {

    public static AbstractPlanNode compileSQL(final Procedure catalog_proc, String name, String sql) throws Exception {
        VoltCompiler compiler = new VoltCompiler();
        HSQLInterface hsql = HSQLInterface.loadHsqldb();
        
        Database catalog_db = (Database)catalog_proc.getParent();
        Catalog catalog = catalog_db.getCatalog();
        Statement catalog_stmt = catalog_proc.getStatements().add(name);
        
        StatementCompiler.compile(compiler, hsql, catalog, catalog_db,
                new DatabaseEstimates(), catalog_stmt, sql, true);
        
        // HACK: For now just return the PlanNodeList from the first fragment
        System.err.println("CATALOG_STMT: " + CatalogUtil.debug(catalog_stmt.getFragments()));
        assert(catalog_stmt.getFragments().get(0) != null);
        String serialized = catalog_stmt.getFragments().get(0).getPlannodetree();
        String jsonString = Encoder.hexDecodeToString(serialized);
        PlanNodeList list = null; // FIXME (PlanNodeList)PlanNodeTree.fromJSONObject(new JSONObject(jsonString), catalog_db);
        return (list.getRootPlanNode());
    }
    
    /**
     * Generate a new catalog object for the given schema
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
        // Setup fake database connection. Pass stuff to database to get catalog objects
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
