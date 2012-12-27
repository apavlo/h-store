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

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = Vote.class;
    private static final int TARGET_BATCH = 1;
    private static final int WORKLOAD_XACT_LIMIT = 1;
    private static final int NUM_PARTITIONS = 50;
    private static final int BASE_PARTITION = 0;
    private static final long TXN_ID = 123l;
    private static final long CLIENT_HANDLE = Long.MAX_VALUE;

    private SQLStmt batch[];
    private ParameterSet args[];
    private Procedure proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.VOTER);
        this.addPartitions(NUM_PARTITIONS);

        this.proc = this.getProcedure(Vote.class);
        this.batch = new SQLStmt[this.proc.getStatements().size()];
        this.args = new ParameterSet[this.batch.length];
        int i = 0;
        for (Statement stmt : this.proc.getStatements()) {
            Object params[] = new Object[stmt.getParameters().size()];
            this.batch[i] = new SQLStmt(stmt);
            this.args[i] = new ParameterSet(params);
            i += 1;
        } // FOR
        
    }
    
    /**
     * testEstimationParametersMaterializedView
     */
    public void testEstimationParametersMaterializedView() throws Exception {
        Statement stmt = null;
        for (Statement s : proc.getStatements()) {
            for (Table tbl : CatalogUtil.getReferencedTables(s)) {
                if (tbl.getMaterializer() != null) {
                    stmt = s;
                    break;
                }
            } // FOR
            if (stmt != null) break;
        } // FOR
        assertNotNull(stmt);
            
        // We should be able to get back info we need for a query
        // that accesses a materialized view table
        int result[] = p_estimator.getStatementEstimationParameters(stmt);
        assertNotNull(stmt.fullName(), result);
        assertEquals(stmt.fullName(), 1, result.length);
    }
    
//    /**
//     * testEstimationParametersReplicated
//     */
//    public void testEstimationParametersReplicated() throws Exception {
//        // Make sure that we can get the statement estimation
//        // parameters for each Statement in the vote procedure
//
//        Statement stmt = null;
//        for (Statement s : proc.getStatements()) {
//            boolean replicatedOnly = true;
//            for (Table tbl : CatalogUtil.getReferencedTables(s)) {
//                if (tbl.getIsreplicated() == false) {
//                    replicatedOnly = false;
//                    break;
//                }
//            } // FOR
//            if (replicatedOnly) {
//                stmt = s;
//                break;
//            }
//        } // FOR
//        assertNotNull(stmt);
//            
//        // The result is allowed to be null if this stmt only references
//        // a replicated table
//        int result[] = p_estimator.getStatementEstimationParameters(stmt);
//        assertNull(stmt.fullName(), result);
//    }
        
}