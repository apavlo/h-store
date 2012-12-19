package edu.brown.designer.partitioners;

import java.io.File;
import java.util.Collection;

import org.junit.Test;
import org.voltdb.catalog.*;
import org.voltdb.types.PartitionMethodType;

import edu.brown.BaseTestCase;
import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;

public class TestPrimaryKeyPartitioner extends BaseTestCase {

    private PrimaryKeyPartitioner partitioner;
    private Workload workload;
    private Designer designer;
    private DesignerInfo info;
    private DesignerHints hints;
    
    private static ParameterMappingsSet correlations;
    private static File correlations_file;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        if (correlations == null) {
            correlations_file = this.getParameterMappingsFile(ProjectType.TPCC);
            assertNotNull(correlations_file);
            assert(correlations_file.exists());
            correlations = new ParameterMappingsSet();
            correlations.load(correlations_file, catalog_db);
        }
        
        // Setup everything else (that's just how we roll up in this ma)
        this.workload = new Workload(catalog);
        this.info = new DesignerInfo(catalogContext, this.workload);
        this.info.setPartitionerClass(PrimaryKeyPartitioner.class);
        this.info.setMappings(correlations);
        this.info.setMappingsFile(correlations_file);
        this.hints = new DesignerHints();

        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (PrimaryKeyPartitioner)this.designer.getPartitioner();
        assertNotNull(this.partitioner);
    }
    
    /**
     * testGenerate
     */
    @Test
    public void testGenerate() throws Exception {
        PartitionPlan pplan = this.partitioner.generate(this.hints);
        assertNotNull(pplan);
        assertEquals(catalogContext.getDataTables().size(), pplan.getTableEntries().size());

        for (Table catalog_tbl : pplan.getTableEntries().keySet()) {
            if (catalog_tbl.getSystable()) continue;
            TableEntry pentry = pplan.getTableEntries().get(catalog_tbl);
            assertNotNull("Null PartitionEntry for " + catalog_tbl, pentry);
            Collection<Column> pkey_columns = CatalogUtil.getPrimaryKeyColumns(catalog_tbl);
            PartitionMethodType expected = (pkey_columns.isEmpty() ? PartitionMethodType.REPLICATION : PartitionMethodType.HASH);
            assertEquals("Invalid PartitionMethodType for " + catalog_tbl, expected, pentry.getMethod());
        } // FOR
        System.err.println(pplan);
    }
}