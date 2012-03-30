package edu.brown.hstore.util;

import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.DeleteCallForwarding;
import edu.brown.utils.ProjectType;

public class TestQueryCache extends BaseTestCase {

    private static final int globalBufferSize = 100;
    private static final int txnBufferSize = 100;
    private static Class<? extends VoltProcedure> TARGET_PROCEDURE = DeleteCallForwarding.class;
    private static String TARGET_STATEMENT = "query";
    
    QueryCache cache;
    Procedure catalog_proc;
    Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        
        this.cache = new QueryCache(globalBufferSize, txnBufferSize);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STATEMENT);
    }
    
    
    
}
