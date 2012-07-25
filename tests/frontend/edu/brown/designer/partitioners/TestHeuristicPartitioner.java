/**
 * 
 */
package edu.brown.designer.partitioners;

import java.io.File;
import java.util.List;
import java.util.Observable;

import org.voltdb.catalog.*;

import edu.brown.BaseTestCase;
import edu.brown.designer.*;
import edu.brown.graphs.IGraph;
import edu.brown.gui.common.GraphVisualizationPanel;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.ProjectType;
import edu.brown.workload.*;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * @author pavlo
 *
 */
public class TestHeuristicPartitioner extends BaseTestCase {

    private static final long WORKLOAD_XACT_LIMIT = 1000;
    private static final String TARGET_PROCEDURE = "neworder";
    private static final int NUM_THREADS = 1;
    
    // Reading the workload takes a long time, so we only want to do it once
    private static Workload workload;

    private Designer designer;
    private DesignerInfo info;
    private DesignerHints hints;
    private HeuristicPartitioner partitioner;
    private Procedure catalog_proc;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TPCC); 
            workload = new Workload(catalog);
            
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter(false);
            filter.include(TARGET_PROCEDURE);
            filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            ((Workload)workload).load(workload_file, catalog_db, filter);
        }
        
        // Setup everything else (that's just how we roll up in this ma)
        this.info = new DesignerInfo(catalogContext, workload);
        this.info.setNumThreads(NUM_THREADS);
        this.info.setPartitionerClass(HeuristicPartitioner.class);
        this.hints = new DesignerHints();
        this.hints.proc_include.add(TARGET_PROCEDURE);

        this.designer = new Designer(this.info, this.hints, this.info.getArgs());
        this.partitioner = (HeuristicPartitioner)this.designer.getPartitioner();
        assertNotNull(this.partitioner);
        
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
    }
    
    private void display(final IGraph<DesignerVertex, DesignerEdge> graph) throws Exception {
        final EventObserver<DesignerVertex> observer = new EventObserver<DesignerVertex>() {
            @Override
            public void update(EventObservable<DesignerVertex> o, DesignerVertex v) {
                if (v == null) return;
                System.err.println(v);
                for (DesignerEdge e : graph.getIncidentEdges(v)) {
                    System.err.println("  " + e + ": " + e.getTotalWeight());
                } // FOR
            }
        };
        
        javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
            public void run() {
                GraphVisualizationPanel.createFrame(graph, observer).setVisible(true);
            }
        });
    }
    
    /**
     * testCreateCandidateRoots
     */
    public void testCreateCandidateRoots() throws Exception {
        final AccessGraph agraph = this.designer.getAccessGraph(this.catalog_proc);
        List<DesignerVertex> candidate_roots = this.partitioner.createCandidateRoots(hints, agraph);
        System.err.println("ROOTS: "+ candidate_roots);
        // display(info.dgraph);
    }
   
    /**
     * testGenerateSinglePartitionTree
     */
    public void testGenerateSinglePartitionTree() throws Exception {
        // Construct the first pass of the PartitionTree
        final PartitionTree ptree = new PartitionTree(catalog_db);
        final Table catalog_tbl = this.getTable("WAREHOUSE");
        final AccessGraph agraph = this.designer.getAccessGraph(this.catalog_proc);
        DesignerVertex parent = agraph.getVertex(catalog_tbl);
        //this.partitioner.generateSinglePartitionTree(this.hints, agraph, ptree, parent);

        // We should have all tables in our tree except for ITEM
//        for (Table child_tbl : catalog_db.getTables()) {
//            if (child_tbl.getName().equals("ITEM")) continue;
//            Vertex child = ptree.getVertex(child_tbl);
//            assertNotNull("Missing vertex for table '" + child_tbl + "' in partition tree", child);
//        } // FOR

        // this.display(agraph);
    }
    
    public static void main(String[] args) throws Exception {
        TestHeuristicPartitioner t = new TestHeuristicPartitioner();
        t.setUp();
        //t.testGenerateSinglePartitionTree();
        t.testCreateCandidateRoots();
    }
    
    
}
