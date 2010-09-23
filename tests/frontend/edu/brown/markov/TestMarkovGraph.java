package edu.brown.markov;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringEscapeUtils;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.correlations.ParameterCorrelations;
import edu.brown.graphs.GraphvizExport;
import edu.brown.graphs.GraphvizExport.Attributes;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractWorkload;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.WorkloadTraceFileOutput;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovGraph extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1000;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 5;

    private static AbstractWorkload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterCorrelations correlations;
    
    private Procedure catalog_proc;
    
    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);
        
        if (markovs == null) {
            File file = this.getCorrelationsFile(ProjectType.TPCC);
            correlations = new ParameterCorrelations();
            correlations.load(file.getAbsolutePath(), catalog_db);
            
            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new WorkloadTraceFileOutput(catalog);
            ProcedureNameFilter filter = new ProcedureNameFilter();
            filter.include(TARGET_PROCEDURE.getSimpleName(), WORKLOAD_XACT_LIMIT);
            
            // Custom filter that only grabs neworders that go to our BASE_PARTITION
            filter.attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION));
            ((WorkloadTraceFileOutput) workload).load(file.getAbsolutePath(), catalog_db, filter);
            
            // Generate MarkovGraphs
            markovs = MarkovUtil.createGraphs(catalog_db, workload, p_estimator);
            assertNotNull(markovs);
        }
    }

    private boolean allUnmarked(MarkovGraph testGraph) {
        for(Edge e:testGraph.getEdges()){
            if(e.marked()) return false;
        }
        return true;
    }

    private boolean allMarked(MarkovGraph testGraph) {
        for(Edge e:testGraph.getEdges()){
            if(!e.marked()) return false;
        }
        return true;
    }
    
    private void validateProbabilities(Vertex v) {
        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            double done = v.getDoneProbability(partition);
            
            // If the DONE probability is 1.0, then the probability that we read/write at
            // a partition must be zero
            if (done == 1.0) {
                assert(0.0d == v.getWriteProbability(partition)) : v + " Partitition #" + partition;
                assert(0.0d == v.getReadOnlyProbability(partition)) : v + " Partitition #" + partition;
                
            // Otherwise, we should at least be reading or writing at this partition with some probability
            } else {
                double sum = v.getWriteProbability(partition) + v.getReadOnlyProbability(partition);
                assert(sum > 0.0d) : v + " Partition #" + partition + " [" + sum + "]";
            }
        } // FOR
        
    }
    
    // ----------------------------------------------------------------------------
    // TEST METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testCalculateProbabilities
     */
    @Test
    public void testCalculateProbabilities() throws Exception {
        MarkovGraph markov = markovs.get(BASE_PARTITION, this.catalog_proc);
        assertNotNull(markov);
        
        // We want to check that the read/write/finish probabilities are properly set
        Vertex start = markov.getStartVertex();
        assertNotNull(start);
        System.err.println(start.debug());
        
        GraphvizExport<Vertex, Edge> graphviz = new GraphvizExport<Vertex, Edge>(markov);
        for (Vertex v : markov.getVertices()) {
            graphviz.getAttributes(v).put(Attributes.LABEL, start.debug().replace("\n", "\\n").replace("\t", "     "));    
        }
        FileUtil.writeStringToFile("/home/pavlo/" + this.catalog_proc.getName() + ".dot", graphviz.export(this.catalog_proc.getName()));
        
        // FIXME validateProbabilities(start);
    }

    /**
     * testAddToEdge
     */
    @Test
    public void testAddToEdge(){
        MarkovGraph testGraph = new MarkovGraph(this.catalog_proc, 0);
        testGraph.initialize();
        Vertex start = testGraph.getStartVertex();
        Vertex stop = testGraph.getCommitVertex();
        
        Statement catalog_stmt = CollectionUtil.getFirst(this.catalog_proc.getStatements());
        for(int i = 0;i<10;i++){
            Vertex current = new Vertex(catalog_stmt, Vertex.Type.QUERY, i, new HashSet<Integer>());
            testGraph.addVertex(current);
            long startcount = start.getTotalHits();
            testGraph.addToEdge(start,current);
            testGraph.addToEdge(current,stop);
            assertTrue(startcount+1 == start.getTotalHits());
            assertTrue(current.getTotalHits() == 1);
        }
        assertTrue(testGraph.isSane());
    }

    /**
     * testUnmarkAllEdges
     */
    @Test
    public void testUnmarkAllEdges() {
        MarkovGraph testGraph = new MarkovGraph(this.catalog_proc, 0);
        for(Edge e: testGraph.getEdges()){
            e.mark();
        }
        assertTrue(allMarked(testGraph));
        testGraph.unmarkAllEdges();
        assertTrue(allUnmarked(testGraph));
    }

    
    /**
     * testGraphSerialization
     */
    public void testGraphSerialization() throws Exception {
        Statement catalog_stmt = this.getStatement(catalog_proc, "getWarehouseTaxRate");
        
        int base_partition = 0;
        MarkovGraph graph = new MarkovGraph(catalog_proc, base_partition);
        graph.initialize();
        
        Vertex v0 = graph.getStartVertex();
        Vertex v1 = new Vertex(catalog_stmt, Vertex.Type.QUERY, 0, CollectionUtil.addAll(new ArrayList<Integer>(), base_partition));
        Vertex v2 = new Vertex(catalog_stmt, Vertex.Type.QUERY, 1, CollectionUtil.addAll(new ArrayList<Integer>(), base_partition));
        graph.addVertex(v1);
        graph.addToEdge(v0, v1);
        graph.addVertex(v2);
        graph.addToEdge(v1, v2);
        
        graph.setTransactionCount(1);
        graph.calculateProbabilities();
        graph.unmarkAllEdges();

        String json = graph.toJSONString();
        assertNotNull(json);
        assertFalse(json.isEmpty());
        JSONObject json_object = new JSONObject(json);
        assertNotNull(json_object);
//        System.err.println(json_object.toString(1));
        
        MarkovGraph clone = new MarkovGraph(catalog_proc, base_partition);
        clone.fromJSON(json_object, catalog_db);
        
        assertEquals(graph.getBasePartition(), clone.getBasePartition());
        assertEquals(graph.getEdgeCount(), clone.getEdgeCount());
        assertEquals(graph.getVertexCount(), clone.getVertexCount());

        // It's lame, but we'll just have to use toString() to match things up
        Map<String, Set<String>> edges = new HashMap<String, Set<String>>();
        for (Edge e : graph.getEdges()) {
            v0 = graph.getSource(e);
            assertNotNull(v0);
            String s0 = v0.toString();
            
            v1 = graph.getDest(e);
            assertNotNull(v1);
            String s1 = v1.toString();
            
            if (!edges.containsKey(s0)) edges.put(s0, new HashSet<String>());
            edges.get(s0).add(s1);
//            System.err.println(v0 + " -> " + v1);
        } // FOR
//        System.err.println("--------------");
        for (Edge e : clone.getEdges()) {
            v0 = clone.getSource(e);
            assertNotNull(v0);
            String s0 = v0.toString();
            
            v1 = clone.getDest(e);
            assertNotNull(v1);
            String s1 = v1.toString();
            
            assert(edges.containsKey(s0));
            assert(edges.get(s0).contains(s1)) : "Invalid edge: " + s0 + " -> " + s1;
            edges.get(s0).remove(s1);
//            System.err.println(v0 + " -> " + v1);
        } // FOR
        for (String s0 : edges.keySet()) {
            assert(edges.get(s0).isEmpty()) : "Missing edges: " + edges.get(s0);
        } // FOR
    }


}
