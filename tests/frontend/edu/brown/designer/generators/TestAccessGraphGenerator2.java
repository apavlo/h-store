/**
 * 
 */
package edu.brown.designer.generators;

import java.io.File;
import org.voltdb.catalog.*;
import org.voltdb.utils.Pair;

import edu.brown.BaseTestCase;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.Edge;
import edu.brown.designer.Vertex;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

/**
 * @author pavlo
 *
 */
public class TestAccessGraphGenerator2 extends BaseTestCase {
    
    private static final long WORKLOAD_XACT_LIMIT = 5000;
    private static final String TARGET_PROCEDURE = "neworder";
    
    // Reading the workload takes a long time, so we only want to do it once
    private static AbstractWorkload workload;
    
    private Procedure catalog_proc;
    private DesignerInfo info;
    
    private AccessGraph agraph;
    private AccessGraphGenerator generator;
    
    @Override
    protected void setUp() throws Exception {
        super.setUp(ProjectType.TPCC, true);
        
        // Super hack! Walk back the directories and find out workload directory
        if (workload == null) {
            File workload_file = this.getWorkloadFile(ProjectType.TPCC); 
            workload = new WorkloadTraceFileOutput(catalog);
        
            // Workload Filter
            ProcedureNameFilter filter = new ProcedureNameFilter();
            filter.include(TARGET_PROCEDURE);
            filter.attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            ((WorkloadTraceFileOutput)workload).load(workload_file.getAbsolutePath(), catalog_db, filter);
        }
        
        // Setup everything else
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        this.info = new DesignerInfo(catalog_db, workload);
        
        this.agraph = new AccessGraph(catalog_db);
        this.generator = new AccessGraphGenerator(this.info, this.catalog_proc);
    }
//    
//    private void display(final AccessGraph agraph) throws Exception {
//          javax.swing.SwingUtilities.invokeAndWait(new Runnable() {
//              public void run() {
//                  GraphVisualizationPanel.createFrame(agraph).setVisible(true);
//              }
//          });
//    }
//    
    /**
     * testGenerate
     */
    @SuppressWarnings("unchecked")
    public void testGenerate() throws Exception {
        // I'm getting weird results when I the AccessGraphGenerator run at Brown. Not sure if it's a 32-bit problem...
        // Just make sure that we get the following edges in the graph
        Pair<Table, Table> expected[] = new Pair[]{
                Pair.of(this.getTable("CUSTOMER"), this.getTable("ORDERS")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("CUSTOMER")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("CUSTOMER"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("DISTRICT")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("ORDERS")),
                Pair.of(this.getTable("DISTRICT"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ITEM"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ITEM"), this.getTable("ITEM")),
                Pair.of(this.getTable("ITEM"), this.getTable("STOCK")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("NEW_ORDER")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("ORDERS")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("NEW_ORDER"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ORDERS"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("ORDERS"), this.getTable("ORDERS")),
                Pair.of(this.getTable("ORDERS"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("ORDER_LINE"), this.getTable("STOCK")),
                Pair.of(this.getTable("ORDER_LINE"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("ORDER_LINE"), this.getTable("ORDER_LINE")),
                Pair.of(this.getTable("STOCK"), this.getTable("STOCK")),
                Pair.of(this.getTable("STOCK"), this.getTable("STOCK")),
                Pair.of(this.getTable("STOCK"), this.getTable("WAREHOUSE")),
                Pair.of(this.getTable("WAREHOUSE"), this.getTable("WAREHOUSE")),
        };
        
        this.generator.generate(agraph);
        for (int i = 0, cnt = expected.length; i < cnt; i++) {
            Pair<Table, Table> search = expected[i];
            Vertex v0 = agraph.getVertex(search.getFirst());
            assertNotNull("[" + i + "]: " + search.toString(), v0);
            Vertex v1 = agraph.getVertex(search.getSecond());
            assertNotNull("[" + i + "]: " + search.toString(), v1);

            Edge found = agraph.findEdge(v0, v1);
            assertNotNull("[" + i + "]: " + search.toString() + "\n", found);
//            System.err.println("REMOVED: " + search + " [" + found + "]");
            agraph.removeEdge(found);
        } // FOR
    }
    
    public static void main(String[] args) throws Exception {
        TestAccessGraphGenerator2 test = new TestAccessGraphGenerator2();
        test.setUp();
        test.testGenerate();
    }
}
