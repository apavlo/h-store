package edu.brown.utils;

import java.util.Collection;

import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.voter.procedures.Vote;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hashing.AbstractHasher;

/**
 * PartitionEstimator tests for queries on views
 * @author pavlo
 */
public class TestPartitionEstimatorViews extends BaseTestCase {
    
    private static final Class<? extends VoltProcedure> TARGET_PROC = Vote.class;
    private static final String TARGET_STMT = "checkVoterStmt";
    private static final long phoneNumber = 5555555555l;
    
    protected static final int NUM_PARTITIONS = 10;
    protected static final int BASE_PARTITION = 1;
    
    private AbstractHasher hasher;
    private final PartitionSet partitions = new PartitionSet();
    private Procedure catalog_proc;
    private Statement catalog_stmt;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.VOTER);
        this.addPartitions(NUM_PARTITIONS);
        
        this.hasher = p_estimator.getHasher();
        this.catalog_proc = this.getProcedure(TARGET_PROC);
        this.catalog_stmt = this.getStatement(catalog_proc, TARGET_STMT);
        
        assertEquals(NUM_PARTITIONS, hasher.getNumPartitions());
        assertEquals(NUM_PARTITIONS, catalogContext.numberOfPartitions);
    }
    
    /**
     * testGetStatementEstimationParameters
     */
    public void testGetStatementEstimationParameters() throws Exception {
        Collection<Table> tables = CatalogUtil.getReferencedTables(catalog_stmt);
        assertEquals(1, tables.size());
        assertNotNull(CollectionUtil.first(tables));
        assertNotNull(CollectionUtil.first(tables).getMaterializer());
        
        int result[] = p_estimator.getStatementEstimationParameters(this.catalog_stmt);
        assertNotNull(catalog_stmt.fullName(), result);
        assertEquals(1, result.length);
    }
    
    /**
     * testGetPartitionsStmtView
     */
    public void testGetPartitionsStmtView() throws Exception {
        Object txn_params[] = new Object[] { BASE_PARTITION, phoneNumber, 1, 100l };
        int procPartition = p_estimator.getBasePartition(catalog_proc, txn_params);
        assert(procPartition >= 0 && procPartition < NUM_PARTITIONS);
        
        // Make sure this is the query that touches the view
        Object stmt_params[] = new Object[] { phoneNumber };
        Collection<Table> stmtTables = CatalogUtil.getReferencedTables(catalog_stmt);
        assertEquals(1, stmtTables.size());
        assertNotNull(CollectionUtil.first(stmtTables).getMaterializer());
        
        // Now if we execute this Statement, it should come back with the
        // same partition as where our Procedure is suppose to execute on
        p_estimator.getAllPartitions(partitions, catalog_stmt, stmt_params, procPartition);
        assertEquals(1, partitions.size());
        assertEquals(procPartition, CollectionUtil.first(partitions).intValue());
    }

}
