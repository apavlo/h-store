package edu.brown.utils;

import java.util.Collection;
import java.util.Set;

import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;

/**
 * PartitionEstimator tests for queries on views
 * @author pavlo
 */
public class TestPartitionEstimatorViews extends BaseTestCase {

    protected static AbstractHasher hasher;
    protected static final int num_partitions = 10;
    protected static final int base_partition = 1;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.VOTER);
        if (hasher == null) {
            this.addPartitions(num_partitions);
            hasher = new DefaultHasher(catalog_db, CatalogUtil.getNumberOfPartitions(catalog_db));
        }
    }
    
    /**
     * testGetPartitionsStmtView
     */
    public void testGetPartitionsStmtView() throws Exception {
        long phoneNumber = 5555555555l;
        Procedure catalog_proc = this.getProcedure(Vote.class);
        Object txn_params[] = new Object[] { phoneNumber, 1, 100l };
        int procPartition = p_estimator.getBasePartition(catalog_proc, txn_params);
        assert(procPartition >= 0 && procPartition < num_partitions);
        
        // Make sure this is the query that touches the view
        Statement catalog_stmt = this.getStatement(catalog_proc, "checkVoterStmt");
        Object stmt_params[] = new Object[] { phoneNumber };
        Collection<Table> stmtTables = CatalogUtil.getReferencedTables(catalog_stmt);
        assertEquals(1, stmtTables.size());
        assertNotNull(CollectionUtil.first(stmtTables).getMaterializer());
        
        // Now if we execute this Statement, it should come back with the
        // same partition as where our Procedure is suppose to execute on
        Set<Integer> stmtPartitions = p_estimator.getAllPartitions(catalog_stmt, stmt_params, procPartition);
        assertEquals(1, stmtPartitions.size());
        assertEquals(procPartition, CollectionUtil.first(stmtPartitions).intValue());
    }

}
