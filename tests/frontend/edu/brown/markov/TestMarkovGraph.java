package edu.brown.markov;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.VoltProcedure;
import org.voltdb.benchmark.tpcc.procedures.neworder;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.MarkovVertex.Type;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.MathUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;
import edu.brown.workload.filters.BasePartitionTxnFilter;
import edu.brown.workload.filters.Filter;
import edu.brown.workload.filters.ProcedureLimitFilter;
import edu.brown.workload.filters.ProcedureNameFilter;

public class TestMarkovGraph extends BaseTestCase {

    private static final Class<? extends VoltProcedure> TARGET_PROCEDURE = neworder.class;
    private static final int WORKLOAD_XACT_LIMIT = 1000;
    private static final int BASE_PARTITION = 1;
    private static final int NUM_PARTITIONS = 20;

    private static Workload workload;
    private static MarkovGraphsContainer markovs;
    private static ParameterMappingsSet correlations;

    private Procedure catalog_proc;

    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TPCC);
        this.addPartitions(NUM_PARTITIONS);
        this.catalog_proc = this.getProcedure(TARGET_PROCEDURE);

        if (markovs == null) {
            File file = this.getParameterMappingsFile(ProjectType.TPCC);
            correlations = new ParameterMappingsSet();
            correlations.load(file, catalogContext.database);

            file = this.getWorkloadFile(ProjectType.TPCC);
            workload = new Workload(catalogContext.catalog);

            // Check out this beauty:
            // (1) Filter by procedure name
            // (2) Filter on partitions that start on our BASE_PARTITION
            // (3) Filter to only include multi-partition txns
            // (4) Another limit to stop after allowing ### txns
            // Where is your god now???
            Filter filter = new ProcedureNameFilter(false).include(TARGET_PROCEDURE.getSimpleName());
            filter.attach(new BasePartitionTxnFilter(p_estimator, BASE_PARTITION))
            // .attach(new MultiPartitionTxnFilter(p_estimator))
                    .attach(new ProcedureLimitFilter(WORKLOAD_XACT_LIMIT));
            workload.load(file, catalogContext.database, filter);
            // assertEquals(WORKLOAD_XACT_LIMIT, workload.getTransactionCount());

            // for (TransactionTrace xact : workload.getTransactions()) {
            // System.err.println(xact + ": " + p_estimator.getAllPartitions(xact));
            // }

            // Generate MarkovGraphs
            markovs = MarkovGraphsContainerUtil.createBasePartitionMarkovGraphsContainer(catalogContext, workload, p_estimator);
            assertNotNull(markovs);
            assertEquals(1, markovs.size());
        }
    }

    private void validateProbabilities(MarkovVertex v) {
//        assertNotNull(v.toString(), v.getSinglePartitionProbability());
//        assert (v.getSinglePartitionProbability() >= 0.0) : "Invalid SingleSited for " + v + ": "
//                + v.getSinglePartitionProbability();
//        assert (v.getSinglePartitionProbability() <= 1.0) : "Invalid SingleSited for " + v + ": "
//                + v.getSinglePartitionProbability();

        assertNotNull(v.toString(), v.getAbortProbability());
        assert (v.getAbortProbability() >= 0.0) : "Invalid Abort for " + v + ": " + v.getAbortProbability();
        assert (v.getAbortProbability() <= 1.0) : "Invalid Abort for " + v + ": " + v.getAbortProbability();

        for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
            final float done = v.getDoneProbability(partition);
            final float write = v.getWriteProbability(partition);
//            final float read_only = v.getReadOnlyProbability(partition);

            Map<MarkovVertex.Probability, Float> probabilities = new HashMap<MarkovVertex.Probability, Float>() {
                private static final long serialVersionUID = 1L;
                {
                    this.put(MarkovVertex.Probability.DONE, done);
                    this.put(MarkovVertex.Probability.WRITE, write);
//                    this.put(MarkovVertex.Probability.READ_ONLY, read_only);
                }
            };
            for (Entry<MarkovVertex.Probability, Float> e : probabilities.entrySet()) {
                assertNotNull("Null " + e.getKey() + " => " + v.toString() + " Partition #" + partition, e.getValue());
                assert (e.getValue() >= 0.0) : "Invalid " + e.getKey() + " for " + v + " at Partition #" + partition + ": " + e.getValue();
                if (e.getValue() > 1) {
                    System.err.println(v.debug());
                }
                assert(MathUtil.lessThanEquals(e.getValue(), 1.0f, MarkovGraph.PROBABILITY_EPSILON)) : "Invalid " + e.getKey() + " for " + v + " at Partition #" + partition + ": " + e.getValue();
            } // FOR

            // If the DONE probability is 1.0, then the probability that we read/write at
            // a partition must be zero
            if (MathUtil.equals(1.0, done, 0.0001)) {
                assertEquals(v + " Partition #" + partition, 0.0f, write, MarkovGraph.PROBABILITY_EPSILON);
//                assertEquals(v + " Partition #" + partition, 1.0f, read_only, MarkovGraph.PROBABILITY_EPSILON);

            // Otherwise, we should at least be reading or writing at this partition with some probability
            } else {
                double sum = write; //  + read_only;
                if (sum == 0) {
                    System.err.println("DONE at Partition #" + partition + " => " + done + " -- " + v.probabilities[MarkovVertex.Probability.DONE.ordinal()][partition]);
                    System.err.println(v.debug());
                }
                assert (sum > 0) : v + " Partition #" + partition + " [" + sum + "]";
            }
        } // FOR
        
        // If this vertex touches a partition that is not the same as the BASE_PARTITION, then 
        // SINGLE_SITED probability should be zero!
        if (v.getType() == Type.QUERY &&
            (v.getPartitions().size() > 1 || v.getPartitions().contains(BASE_PARTITION) == false)) {
//            assertEquals(v.toString(), 0.0f, v.getSinglePartitionProbability(), MarkovGraph.PROBABILITY_EPSILON);
        }

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
        MarkovVertex start = markov.getStartVertex();
        MarkovVertex commit = markov.getCommitVertex();
        assertNotNull(start);
        assertNotNull(commit);
        // System.err.println(start.debug());

        // System.err.println("Single-Sited: " + start.getSingleSitedProbability());
        // System.err.println("Abort:        " + start.getAbortProbability());

//        MarkovUtil.exportGraphviz(markov, true, null).writeToTempFile(catalog_proc);

        for (MarkovVertex v : markov.getVertices()) {
            validateProbabilities(v);
        }
        
//        // Double-check that all of the vertices adjacent to the COMMIT vertex have their DONE
//        // probability set to 1.0 if they don't touch the partition. And if they have only one 
//        // partition then it should be single-partitioned
//        for (MarkovVertex v : markov.getPredecessors(commit)) {
//            Collection<Integer> partitions = v.getPartitions();
//            assertFalse(v.toString(), partitions.isEmpty());
//            
//            // MULTI-PARTITION
//            if (partitions.size() > 1) {
////                assertEquals(v.toString(), 0.0f, v.getSinglePartitionProbability());
//            }
//            
//            for (int partition = 0; partition < NUM_PARTITIONS; partition++) {
//                String debug = "PARTITION:"+partition+"\n"+v.debug();
//                if (partitions.contains(partition)) {
//                    assertTrue(debug, v.getDoneProbability(partition) < 1.0f);
//                // We can only do this check if the vertex does not have edges to another vertex
//                } else if (markov.getSuccessorCount(v) == 1) {
//                    assertEquals(debug, 1.0f, v.getDoneProbability(partition), 0.001);
//                }
//            } // FOR
//        } // FOR

    }

    /**
     * testAddToEdge
     */
    @Test
    public void testAddToEdge() throws Exception {
        MarkovGraph testGraph = new MarkovGraph(this.catalog_proc);
        testGraph.initialize();
        MarkovVertex start = testGraph.getStartVertex();
        MarkovVertex stop = testGraph.getCommitVertex();

        Statement catalog_stmt = CollectionUtil.first(this.catalog_proc.getStatements());
        PartitionSet all_previous = new PartitionSet();
        for (int i = 0; i < 10; i++) {
            PartitionSet partitions = new PartitionSet();
            partitions.add(i % NUM_PARTITIONS);
            PartitionSet previous = new PartitionSet(all_previous);
            
            MarkovVertex current = new MarkovVertex(catalog_stmt, MarkovVertex.Type.QUERY, i, partitions, previous);
            testGraph.addVertex(current);
            
            long startcount = start.getInstanceHits();
            testGraph.addToEdge(start, current);
            MarkovEdge e = testGraph.addToEdge(current, stop);
            
            start.incrementInstanceHits();
            current.incrementInstanceHits();
            e.incrementInstanceHits();
            
            assertEquals(startcount + 1, start.getInstanceHits());
            assertEquals(1, current.getInstanceHits());
            all_previous.addAll(partitions);
        }
        
        testGraph.calculateProbabilities(catalogContext.getAllPartitionIds());
        
//        if (testGraph.isValid() == false) {
//            System.err.println("FAILED: " + MarkovUtil.exportGraphviz(testGraph, true, null).writeToTempFile());
//        }
        testGraph.validate();
    }

        
     /**
     * testGraphSerialization
     */
     public void testGraphSerialization() throws Exception {
         Statement catalog_stmt = this.getStatement(catalog_proc, "getWarehouseTaxRate");
                
         MarkovGraph graph = new MarkovGraph(catalog_proc);
         graph.initialize();
                
         MarkovVertex v0 = graph.getStartVertex();
         MarkovVertex v1 = new MarkovVertex(catalog_stmt, MarkovVertex.Type.QUERY, 0, PartitionSet.singleton(BASE_PARTITION),PartitionSet.singleton(BASE_PARTITION));
         MarkovVertex v2 = new MarkovVertex(catalog_stmt, MarkovVertex.Type.QUERY, 1, PartitionSet.singleton(BASE_PARTITION),PartitionSet.singleton(BASE_PARTITION));

         MarkovVertex last = null;
         for (MarkovVertex v : new MarkovVertex[]{v0, v1, v2, graph.getCommitVertex(), null}) {
             if (v == null) break;
             
             if (v.isQueryVertex()) graph.addVertex(v);
             v.incrementInstanceHits();
             if (last != null) {
                 graph.addToEdge(last, v).incrementInstanceHits();        
             }
             last = v;
         } // FOR
         graph.setTransactionCount(1);
         graph.calculateProbabilities(catalogContext.getAllPartitionIds());
        
         String json = graph.toJSONString();
         assertNotNull(json);
         assertFalse(json.isEmpty());
         JSONObject json_object = new JSONObject(json);
         assertNotNull(json_object);
         // System.err.println(json_object.toString(1));
                
         MarkovGraph clone = new MarkovGraph(catalog_proc);
         clone.fromJSON(json_object, catalogContext.database);
                
//         assertEquals(graph.getBasePartition(), clone.getBasePartition());
         assertEquals(graph.getEdgeCount(), clone.getEdgeCount());
         assertEquals(graph.getVertexCount(), clone.getVertexCount());
        
         // It's lame, but we'll just have to use toString() to match things up
         Map<String, Set<String>> edges = new HashMap<String, Set<String>>();
         for (MarkovEdge e : graph.getEdges()) {
             v0 = graph.getSource(e);
             assertNotNull(v0);
             String s0 = v0.toString();
                        
             v1 = graph.getDest(e);
             assertNotNull(v1);
             String s1 = v1.toString();
                        
             if (!edges.containsKey(s0)) edges.put(s0, new HashSet<String>());
             edges.get(s0).add(s1);
             System.err.println(v0 + " -> " + v1);
         } // FOR
         System.err.println("--------------");
         for (MarkovEdge e : clone.getEdges()) {
             v0 = clone.getSource(e);
             assertNotNull(v0);
             String s0 = v0.toString();
                        
             v1 = clone.getDest(e);
             assertNotNull(v1);
             String s1 = v1.toString();
              
             System.err.println(v0 + " -> " + v1);
             assert(edges.containsKey(s0)) : edges;
             assert(edges.get(s0).contains(s1)) : "Invalid edge: " + s0 + " -> " + s1;
             edges.get(s0).remove(s1);
         } // FOR
         for (String s0 : edges.keySet()) {
             assert(edges.get(s0).isEmpty()) : "Missing edges: " + edges.get(s0);
         } // FOR
     }
}
