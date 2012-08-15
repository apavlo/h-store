package edu.brown.designer.partitioners;

import java.util.HashSet;

import org.voltdb.catalog.Table;

import edu.brown.benchmark.tm1.TM1Constants;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.generators.AccessGraphGenerator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public class TestAbstractPartitioner extends BasePartitionerTestCase {

    public static class MockPartitioner extends AbstractPartitioner {
        
        public MockPartitioner(Designer designer, DesignerInfo info) {
            super(designer, info);
        }
        @Override
        public PartitionPlan generate(DesignerHints hints) throws Exception {
            return null;
        }
    }
    
    private MockPartitioner partitioner;
    private AccessGraph agraph;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TM1, true);
        
        // BasePartitionerTestCase will setup most of what we need
        this.info.setCostModel(new TimeIntervalCostModel<SingleSitedCostModel>(catalogContext, SingleSitedCostModel.class, info.getNumIntervals()));
        this.info.setPartitionerClass(MockPartitioner.class);
        assertNotNull(info.getStats());
        
        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (MockPartitioner) this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        this.agraph = AccessGraphGenerator.convertToSingleColumnEdges(catalog_db, this.partitioner.generateAccessGraph());
        assertNotNull(this.agraph);
    }
    
    /**
     * testGenerateAccessGraph
     */
    public void testGenerateAccessGraph() throws Exception {
        // Make sure all of our tables are there
        for (Table catalog_tbl : catalog_db.getTables()) {
            DesignerVertex v = agraph.getVertex(catalog_tbl);
            assertNotNull("Missing " + catalog_tbl, v);
        } // FOR
        
        // Make sure our edges have weights
        // Only the self-referencing edges for CALL_FORWARDING and SUBSCRIBER will be zero
        HashSet<Table> skip = new HashSet<Table>();
        skip.add(this.getTable(TM1Constants.TABLENAME_SUBSCRIBER));
        skip.add(this.getTable(TM1Constants.TABLENAME_CALL_FORWARDING));
        for (DesignerEdge e : agraph.getEdges()) {
            DesignerVertex v0 = CollectionUtil.get(agraph.getIncidentVertices(e), 0);
            DesignerVertex v1 = CollectionUtil.get(agraph.getIncidentVertices(e), 1);
            if (!(v0.getCatalogItem().equals(v1.getCatalogItem()) && skip.contains(v0.getCatalogItem()))) {
                assert(e.getTotalWeight() > 0) : "No edge weight for " + e;
            }
        } // FOR
    }
    

    

}