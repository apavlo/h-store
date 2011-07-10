package edu.brown.workload;

import java.util.*;

import org.json.JSONObject;
import org.voltdb.catalog.*;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.Pair;

public class TestQueryTrace extends TestTransactionTrace {

    protected List<QueryTrace> query_traces = new ArrayList<QueryTrace>();
    protected List<Object[]> query_params = new ArrayList<Object[]>();
    protected final static int NUM_OF_QUERIES = 3; 
    
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();

        Statement stmts[] = catalog_proc.getStatements().values();
        for (int i = 0; i < NUM_OF_QUERIES; i++) {
            int idx = rand.nextInt(stmts.length);
//            System.out.println("idx=" + idx);
//            System.out.println("num_of_stmts=" + num_of_stmts);
//            System.out.println(catalog_proc.getStatements().size());
            Statement catalog_stmt = stmts[idx];
            assertNotNull(catalog_stmt);
            
            List<StmtParameter> catalog_params = CatalogUtil.getSortedCatalogItems(catalog_stmt.getParameters(), "index");
            Pair<Object[], boolean[]> param_pair = this.makeParams(catalog_params, "javatype");
            this.query_params.add((Object[])param_pair.getFirst());
            this.query_traces.add(new QueryTrace(catalog_stmt, query_params.get(i), 0));
            this.xact.getQueries().add(this.query_traces.get(i));
        } // FOR
    }
    
    @Override
    public void testToJSONString() throws Exception {
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        
        //
        // Check to make sure our queries got serialized too
        //
        for (QueryTrace.Members element : QueryTrace.Members.values()) {
            assertTrue(json.indexOf(element.name()) != -1);
        } // FOR
    }

    public void testFromJSONString() throws Exception {
        String json = xact.toJSONString(catalog_db);
        assertNotNull(json);
        JSONObject jsonObject = new JSONObject(json);
//        System.out.println(jsonObject.toString(2));
//        System.exit(1);
        
        TransactionTrace copy = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
        assertEquals(xact.getQueries().size(), copy.getQueries().size());
        
        for (int i = 0, cnt = xact.getQueries().size(); i < cnt; i++) {
            QueryTrace query = xact.getQueries().get(i);
            QueryTrace copy_query = copy.getQueries().get(i);
            
            assertEquals(query.catalog_item_name, copy_query.catalog_item_name);
            assertEquals(query.getBatchId(), copy_query.getBatchId());
            assertEquals(query.start_timestamp, copy_query.start_timestamp);
        
            assertEquals(query.params.length, copy_query.params.length);
            for (int j = 0; j < query.params.length; j++) {
                this.compare(query.params[j], copy_query.params[j]);    
            } // FOR
        } // FOR
    }
}
