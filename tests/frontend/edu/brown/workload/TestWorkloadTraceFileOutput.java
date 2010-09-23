package edu.brown.workload;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;

import org.voltdb.catalog.*;

import edu.brown.utils.*;
import edu.brown.BaseTestCase;

public class TestWorkloadTraceFileOutput extends BaseTestCase {

    public static final Random rand = new Random();
    
    public static final String TARGET_PROCEDURE = "neworder";
    public static final int NUM_ORDER_ITEMS = 5;
    public static final int BASE_PARTITION = 1;
    public static final String CALLER = "XXX"; 

    public static final Object BASE_ARGS[] = new Object[]{
        new Long(BASE_PARTITION),   // [0] W_ID
        new Long(1),                // [1] D_ID
        new Long(1),                // [2] C_ID
        new Date(),                 // [3] TIMESTAMP    
        new Long[NUM_ORDER_ITEMS],  // [4] OL_I_IDs
        new Long[NUM_ORDER_ITEMS],  // [5] OL_SUPPLY_W_ID
        new Long[NUM_ORDER_ITEMS],  // [6] OL_QUANTITY
    };
    static {
        Long ids0[] = (Long[])BASE_ARGS[4];
        Long ids1[] = (Long[])BASE_ARGS[6];
        for (int i = 0; i < NUM_ORDER_ITEMS; i++) {
            ids0[i] = new Long(rand.nextLong());
            ids1[i] = new Long(rand.nextLong());
        } // FOR
    };
    
    protected WorkloadTraceFileOutput workload;
    protected Procedure catalog_proc;
    protected Object single_xact_args[] = new Object[BASE_ARGS.length];
    protected Object multi_xact_args[] = new Object[BASE_ARGS.length];
    protected TransactionTrace last_xact_trace = null;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        
        this.workload = new WorkloadTraceFileOutput(catalog);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        //
        // Create arguments for two xact traces, one that is single-partition and one that is multi-partition
        //
        for (int i = 0; i < 2; i++) {
            Object args[] = (i == 0 ? this.single_xact_args : this.multi_xact_args);
            for (int ii = 0; ii < args.length; ii++) {
                if (ii != 5) {
                    args[ii] = BASE_ARGS[ii];
                } else {
                    Long w_ids[] = new Long[NUM_ORDER_ITEMS];
                    
                    // Single-Partition
                    if (i == 0) {
                        for (int iii = 0; iii < NUM_ORDER_ITEMS; iii++) {
                            w_ids[iii] = new Long(BASE_PARTITION);
                        } // FOR
                    // Multi-Partition
                    } else {
                        for (int iii = 0; iii < NUM_ORDER_ITEMS; iii++) {
                            int rand_idx = rand.nextInt(10);
                            if (rand_idx <= 5) {
                                w_ids[iii] = new Long(BASE_PARTITION);
                            } else {
                                w_ids[iii] = new Long(rand_idx % 5);
                            }
                        } // FOR
                    }
                    args[ii] = w_ids;
                }
            } // FOR
        } // FOR
    }
    
    protected TransactionTrace startTransaction(Object args[]) {
        TransactionTrace xact = this.workload.startTransaction(CALLER, this.catalog_proc, args);
        assertNotNull(xact);
        assertNotNull(xact.getId());
        assertTrue(xact.getId() >= 0);
        assertNotNull(xact.getStartTimestamp());
        assertTrue(xact.getStopTimestamp() == null);
        this.last_xact_trace = xact;
        return (xact);
    }
    
    protected QueryTrace startQuery(TransactionTrace xact, Statement catalog_stmt, Object args[], int batch_id) {
        assertNotNull(xact);
        QueryTrace query = (QueryTrace)this.workload.startQuery(xact, catalog_stmt, args, batch_id);
        assertNotNull(query);
        assertNotNull(query.getId());
        assertTrue(query.getId() >= 0);
        assertNotNull(query.getStartTimestamp());
        assertTrue(query.getStopTimestamp() == null);
        assertEquals(catalog_stmt.getName(), query.getCatalogItemName());
        assertEquals(batch_id, query.getBatchId());
        assertEquals(args.length, query.getParams().length);
        return (query);
    }
    
    /**
     * testStartTransaction
     */
    public void testStartTransaction() throws Exception {
        TransactionTrace xact = this.startTransaction(this.single_xact_args);
        assertNotNull(this.workload.getTraceObject(xact.getId()));
    }
    
    /**
     * testStopTransaction
     */
    public void testStopTransaction() throws Exception {
        TransactionTrace xact = this.startTransaction(this.single_xact_args);
        
        this.workload.stopTransaction(xact);
        assertNotNull(xact.getStopTimestamp());
    }
    
    /**
     * testStartQuery
     */
    public void testStartQuery() throws Exception {
        TransactionTrace xact = this.startTransaction(this.single_xact_args);
        
        List<Statement> stmt_objs = new ArrayList<Statement>();
        List<Object[]> stmt_args = new ArrayList<Object[]>();
        List<Integer> stmt_batchids = new ArrayList<Integer>();
        
        String stmt_names[] = { "getStockInfo", "getWarehouseTaxRate", "getDistrict" };
        for (String stmt_name : stmt_names) {
            int batch_id = this.workload.getNextBatchId(xact);

            if (stmt_name.equals("getStockInfo")) {
                assertEquals(0, batch_id);
                int item_idx = 0;
                for (Statement catalog_stmt : catalog_proc.getStatements()) {
                    if (!catalog_stmt.getName().startsWith(stmt_name)) continue;
                    
                    Object args[] = new Object[] {
                        ((Long[])this.single_xact_args[4])[item_idx],
                        ((Long[])this.single_xact_args[5])[item_idx],
                    };
                    stmt_objs.add(catalog_stmt);
                    stmt_args.add(args);
                    stmt_batchids.add(batch_id);
                    
                    if (item_idx++ >= (NUM_ORDER_ITEMS - 1)) break;
                } // FOR
            } else if (stmt_name.equals("getWarehouseTaxRate")) {
                Object args[] = new Object[] {
                    this.single_xact_args[1],   // W_ID
                };
                stmt_objs.add(catalog_proc.getStatements().get(stmt_name));
                stmt_args.add(args);
                stmt_batchids.add(batch_id);
            } else if (stmt_name.equals("getDistrict")) {
                Object args[] = new Object[] {
                    this.single_xact_args[2],   // D_ID
                    this.single_xact_args[1],   // W_ID
                };
                stmt_objs.add(catalog_proc.getStatements().get(stmt_name));
                stmt_args.add(args);
                stmt_batchids.add(batch_id);
            }
        } // FOR
        
        for (int i = 0, cnt = stmt_objs.size(); i < cnt; i++) {
            Statement catalog_stmt = stmt_objs.get(i);
            Object args[] = stmt_args.get(i);
            int batch_id = stmt_batchids.get(i);
            
            QueryTrace query = this.startQuery(xact, catalog_stmt, args, batch_id);
            assertNotNull(query);
            assertEquals(xact.getTransactionId(), query.getTransactionId());
            this.workload.stopQuery(query);
        } // FOR
        
        this.workload.stopTransaction(xact);
        assertNotNull(xact.getStopTimestamp());        
    }

    
}
