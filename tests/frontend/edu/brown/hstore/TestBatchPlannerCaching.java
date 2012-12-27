package edu.brown.hstore;

import java.util.Arrays;
import java.util.List;

import org.apache.tools.ant.taskdefs.optional.junit.BaseTest;
import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;

/**
 * Additional PartitionEstimator Tests
 * @author pavlo
 */
public class TestBatchPlannerCaching extends BaseTestCase {

    private static final int TARGET_BATCH = 1;
    private static final int WORKLOAD_XACT_LIMIT = 1;
    private static final int NUM_PARTITIONS = 50;
    private static final int BASE_PARTITION = 0;
    private static final long TXN_ID = 123l;
    private static final long CLIENT_HANDLE = Long.MAX_VALUE;

    private SQLStmt batch[];
    private ParameterSet args[];
    private Procedure proc;
    private BatchPlanner planner;
    private BatchPlanner.Debug plannerDebug;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.VOTER);
        this.addPartitions(NUM_PARTITIONS);

        this.proc = this.getProcedure(Vote.class);
        this.batch = new SQLStmt[this.proc.getStatements().size()-1];
        this.args = new ParameterSet[this.batch.length];
        int i = 0;
        for (Statement stmt : this.proc.getStatements()) {
            // Skip the INSERT query because that's the only one where
            // the partitioning parameter actually matters
            if (stmt.getQuerytype() == QueryType.INSERT.getValue()) continue;
            
            Object params[] = this.randomStatementParameters(stmt);
            this.batch[i] = new SQLStmt(stmt);
            this.args[i] = new ParameterSet(params);
            i += 1;
        } // FOR
        
        this.planner = new BatchPlanner(this.batch, proc, p_estimator);
        this.plannerDebug = this.planner.getDebugContext();
    }
    
    /**
     * testSelectMaterializedView
     */
    public void testSelectMaterializedView() throws Exception {
        Statement stmt = null;
        int stmt_index = -1;
        for (int i = 0; i < this.batch.length; i++) {
            Statement s = this.batch[i].getStatement();
            for (Table tbl : CatalogUtil.getReferencedTables(s)) {
                if (tbl.getMaterializer() != null) {
                    stmt = s;
                    stmt_index = i;
                    break;
                }
            } // FOR
            if (stmt != null) break;
        } // FOR
        assertNotNull(stmt);
        assertTrue(stmt_index >= 0);
            
        // We should be able to get back info we need for a query
        // that accesses a materialized view table
        int result[] = this.plannerDebug.getCachedLookup(stmt_index);
        assertNotNull(stmt.fullName(), result);
        assertEquals(stmt.fullName(), 1, result.length);
        assertTrue(stmt.fullName(), this.plannerDebug.isCachedReadOnly(stmt_index));
        assertFalse(stmt.fullName(), this.plannerDebug.isCachedReplicatedOnly(stmt_index));
    }
    
    /**
     * testSelectReplicated
     */
    public void testSelectReplicated() throws Exception {
        Statement stmt = null;
        int stmt_index = -1;
        for (int i = 0; i < this.batch.length; i++) {
            Statement s = this.batch[i].getStatement();
            boolean replicatedOnly = true;
            for (Table tbl : CatalogUtil.getReferencedTables(s)) {
                if (tbl.getIsreplicated() == false) {
                    replicatedOnly = false;
                    break;
                }
            } // FOR
            if (replicatedOnly) {
                stmt = s;
                stmt_index = i;
                break;
            }
        } // FOR
        assertNotNull(stmt);
        assertTrue(stmt_index >= 0);
            
        // The result is allowed to be null if this stmt only references
        // a replicated table
        int result[] = this.plannerDebug.getCachedLookup(stmt_index);
        assertNotNull(stmt.fullName(), result);
        assertEquals(stmt.fullName(), 1, result.length);
        assertTrue(stmt.fullName(), this.plannerDebug.isCachedReadOnly(stmt_index));
        assertFalse(stmt.fullName(), this.plannerDebug.isCachedReplicatedOnly(stmt_index));
    }
        
}