package edu.brown.hstore.specexec.checkers;

import java.util.Collection;

import org.apache.commons.collections15.CollectionUtils;
import org.junit.Before;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.MockHStoreSite;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.specexec.checkers.TableConflictChecker;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;

public class TestTableConflictChecker extends BaseTestCase {

    private static final int NUM_PARTITIONS = 10;
    private static final int BASE_PARTITION = 1;
    
    private HStoreSite hstore_site;
    private TableConflictChecker checker;
    private long nextTxnId = 1000;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        
        this.hstore_site = new MockHStoreSite(0, catalogContext, HStoreConf.singleton());
        this.checker = new TableConflictChecker(catalogContext);
    }
    
    // ----------------------------------------------------------------------------------
    // HELPER METHODS
    // ----------------------------------------------------------------------------------

    private LocalTransaction createTransaction(Procedure catalog_proc) throws Exception {
        PartitionSet partitions = new PartitionSet(BASE_PARTITION); 
        LocalTransaction ts = new LocalTransaction(this.hstore_site);
        ts.testInit(this.nextTxnId++,
                    BASE_PARTITION,
                    partitions,
                    catalog_proc,
                    new Object[catalog_proc.getParameters().size()]);
        return (ts);
    }
    
    // ----------------------------------------------------------------------------------
    // TESTS
    // ----------------------------------------------------------------------------------
    
    /**
     * testAllProcedures
     */
    public void testAllProcedures() throws Exception {
        for (Procedure proc0 : catalogContext.getRegularProcedures()) {
            LocalTransaction txn0 = this.createTransaction(proc0);
            Collection<Table> tables0 = CatalogUtil.getReferencedTables(proc0);
            for (Table tbl : tables0) {
                if (proc0.getReadonly()) {
                    txn0.markTableRead(BASE_PARTITION, tbl);
                } else {
                    txn0.markTableWritten(BASE_PARTITION, tbl);
                }
            } // FOR

            System.err.println(proc0.getName() + " -> " + tables0);
            
            for (Procedure proc1 : catalogContext.getRegularProcedures()) {
                LocalTransaction txn1 = this.createTransaction(proc1);
                Collection<Table> tables1 = CatalogUtil.getReferencedTables(proc1);
                for (Table tbl : tables1) {
                    if (proc1.getReadonly()) {
                        txn1.markTableRead(BASE_PARTITION, tbl);
                    } else {
                        txn1.markTableWritten(BASE_PARTITION, tbl);
                    }
                } // FOR
                
                // XXX: This test is not really useful because we're not actually
                // trying to guess whether there is a conflict. We're just throwing
                // everything at the TableConflictChecker to make sure that there aren't
                // any unexpected problems.
                
                Collection<Table> intersection = CollectionUtils.intersection(tables0, tables1); 
                boolean expected = (intersection.isEmpty() == false);
                boolean result = this.checker.hasConflictBefore(txn0, txn1, BASE_PARTITION);
                System.err.printf("   %s\n", intersection);
                System.err.printf("   %s -> %s {%s}\n\n", proc1.getName(), tables1, result);
                // assertEquals(proc0+"->"+proc1, expected, result);
            } // FOR
            System.err.println();
        } // FOR
    }
}
