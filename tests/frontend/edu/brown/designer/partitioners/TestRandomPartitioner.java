package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map.Entry;

import org.junit.Test;
import org.voltdb.catalog.*;
import org.voltdb.types.PartitionMethodType;

import edu.brown.BaseTestCase;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.WorkloadTraceFileOutput;

public class TestRandomPartitioner extends BaseTestCase {

    private RandomPartitioner partitioner;
    private AbstractWorkload workload;
    private Designer designer;
    private DesignerInfo info;
    private DesignerHints hints;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // Setup everything else (that's just how we roll up in this ma)
        this.workload = new WorkloadTraceFileOutput(catalog);
        this.info = new DesignerInfo(catalog_db, this.workload);
        this.info.setPartitionerClass(RandomPartitioner.class);
        this.hints = new DesignerHints();

        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (RandomPartitioner)this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.partitioner.setLimitedColumns(false);
    }
    
    /**
     * testGenerate
     */
    @Test
    public void testGenerate() throws Exception {
        // Just make sure that something got picked for every table+procedure
        PartitionPlan pplan = this.partitioner.generate(this.hints);
        assertNotNull(pplan);
        assertEquals(catalog_db.getTables().size(), pplan.getTableEntries().size());
        Collection<PartitionMethodType> allowed_types = null;
        
        // TABLES
        allowed_types = CollectionUtil.addAll(new HashSet<PartitionMethodType>(), PartitionMethodType.HASH,
                                                                                  PartitionMethodType.REPLICATION,
                                                                                  PartitionMethodType.MAP);
        for (Entry<Table,PartitionEntry> e : pplan.getTableEntries().entrySet()) {
            assertNotNull(e.getKey());
            assertNotNull("Null PartitionEntry for " + e.getKey(), e.getValue());
            assert(allowed_types.contains(e.getValue().getMethod())) : "Unexpected: " + e.getValue().getMethod();
            if (e.getValue().getMethod() != PartitionMethodType.REPLICATION) assertNotNull("Null attribute for " + e.getValue(), e.getValue().getAttribute());
        } // FOR
        
        // PROCEDURES
        allowed_types = CollectionUtil.addAll(new HashSet<PartitionMethodType>(), PartitionMethodType.HASH,
                                                                                  PartitionMethodType.NONE);
        for (Entry<Procedure, PartitionEntry> e : pplan.getProcedureEntries().entrySet()) {
            assertNotNull(e.getKey());
            assertNotNull("Null PartitionEntry for " + e.getKey(), e.getValue());
            assert(allowed_types.contains(e.getValue().getMethod())) : "Unexpected: " + e.getValue().getMethod();
            if (e.getValue().getMethod() != PartitionMethodType.NONE) assertNotNull(e.getValue().getAttribute());
        } // FOR
        
        // System.err.println(pplan);
    }
}