package edu.brown.catalog.conflicts;

import org.junit.Test;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.benchmark.tpcc.procedures.GetTableCounts;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerId;
import org.voltdb.benchmark.tpcc.procedures.paymentByCustomerName;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.conflicts.ConflictSetCalculator;

public class TestConflictSetCalculator extends BaseTestCase {
    
    ConflictSetCalculator cc;
    
    private final AbstractProjectBuilder builder = new TPCCProjectBuilder() {
        {
            this.addAllDefaults();
            this.addStmtProcedure(
                "NonConflictRead",
                "SELECT W_ZIP FROM WAREHOUSE WHERE W_NAME = ?"
            );
            this.addStmtProcedure(
                "NonConflictUpdate",
                "UPDATE WAREHOUSE SET W_STREET_1 = ? WHERE W_ID = ?"
            );
        }
    };
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(builder);
        this.cc = new ConflictSetCalculator(catalog);
    }
    
    /**
     * testReadReadNonConflict
     */
    @Test
    public void testReadReadNonConflict() throws Exception {
        Procedure proc0 = this.getProcedure("NonConflictRead");
        Procedure proc1 = this.getProcedure(slev.class);
        
        boolean conflicts = !this.cc.checkReadWriteConflict(proc0, proc1).isEmpty();
        assertFalse(conflicts);
    }
    
    /**
     * testReadWriteUpdateNonConflict
     */
    @Test
    public void testReadWriteUpdateNonConflict() throws Exception {
        Procedure proc0 = this.getProcedure("NonConflictRead");
        Procedure proc1 = this.getProcedure("NonConflictUpdate");
        
        boolean conflicts = !this.cc.checkReadWriteConflict(proc0, proc1).isEmpty();
        assertFalse(conflicts);
    }
    
    /**
     * testReadWriteConflict
     */
    @Test
    public void testReadWriteConflict() throws Exception {
        Procedure proc0 = this.getProcedure(neworder.class);
        Procedure proc1 = this.getProcedure(slev.class);
        
        // There is no conflict between #1 and #2
        boolean conflicts = !this.cc.checkReadWriteConflict(proc0, proc1).isEmpty();
        assertFalse(conflicts);
        
        // But there should be one between #2 and #1
        conflicts = !this.cc.checkReadWriteConflict(proc1, proc0).isEmpty();
        assertTrue(conflicts);
    }
    
    /**
     * testWriteWriteConflict
     */
    @Test
    public void testWriteWriteConflict() throws Exception {
        Procedure proc0 = this.getProcedure(paymentByCustomerId.class);
        Procedure proc1 = this.getProcedure(paymentByCustomerName.class);
        
        boolean conflicts = !this.cc.checkWriteWriteConflict(proc0, proc1).isEmpty();
        assertTrue(conflicts);
        
        // Should be symmetrical
        conflicts = !this.cc.checkWriteWriteConflict(proc1, proc0).isEmpty();
        assertTrue(conflicts);
    }
    
    /**
     * testAllConflicts
     */
    @Test
    public void testAllConflicts() throws Exception {
        Procedure proc0 = this.getProcedure(neworder.class);
        Procedure proc1 = this.getProcedure(GetTableCounts.class);
        boolean conflicts;
        
        conflicts = !this.cc.checkReadWriteConflict(proc0, proc1).isEmpty();
        assertFalse(conflicts);
        conflicts = !this.cc.checkReadWriteConflict(proc1, proc0).isEmpty();
        assertTrue(conflicts);
        
        conflicts = !this.cc.checkWriteWriteConflict(proc0, proc1).isEmpty();
        assertFalse(conflicts);
        conflicts = !this.cc.checkWriteWriteConflict(proc1, proc0).isEmpty();
        assertFalse(conflicts);
    }

}
