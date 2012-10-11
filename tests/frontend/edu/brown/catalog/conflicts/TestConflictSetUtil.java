package edu.brown.catalog.conflicts;

import java.util.Collection;

import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.benchmark.tpcc.procedures.slev;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestConflictSetUtil extends BaseTestCase {

    private static final int NUM_PARTITIONS = 6;

    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
    }
    
    /**
     * testGetReadWriteConflicts
     */
    public void testGetConflictProcedures() throws Exception {
        // We expect there to be a conflict between slev and neworder
        Procedure proc0 = this.getProcedure(neworder.class);
        Procedure proc1 = this.getProcedure(slev.class);
        
        // slev doesn't write anything, so there should be no READ-WRITE conflict
        Collection<Procedure> conflicts = ConflictSetUtil.getReadWriteConflicts(proc0);
        assertNotNull(conflicts);
        assertFalse(proc0+": "+conflicts.toString(), conflicts.contains(proc1));
        
        // But it's not symmetrical, because neworder writes some stuff out
        conflicts = ConflictSetUtil.getReadWriteConflicts(proc1);
        assertNotNull(conflicts);
        assertFalse(conflicts.contains(proc1));
        assertTrue(conflicts.contains(proc0));
    }
    
    /**
     * testGetWriteWriteConflicts
     */
    public void testGetWriteWriteConflicts() throws Exception {
        // For each Procedure that is marked as conflicting with another,
        // make sure that they each know about each other
        for (Procedure proc0 : catalog_db.getProcedures()) {
            Collection<Procedure> conflicts0 = ConflictSetUtil.getWriteWriteConflicts(proc0);
            
            // If the proc is read-only, then it should never have a write-write conflict
            if (proc0.getReadonly()) {
                assertEquals(proc0.getName(), 0, conflicts0.size());
            }
            
            for (Procedure proc1 : conflicts0) {
                Collection<Procedure> conflicts1 = ConflictSetUtil.getWriteWriteConflicts(proc1);
                assertTrue(proc0 + "<->" + proc1, conflicts1.contains(proc0));
            } // FOR
        } // FOR
    }
    
}
