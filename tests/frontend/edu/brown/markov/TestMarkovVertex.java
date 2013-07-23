package edu.brown.markov;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.graphs.AbstractVertex;
import edu.brown.markov.MarkovVertex.Type;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ProjectType;
import edu.brown.workload.Workload;

/**
 * 
 * @author svelagap
 * @author pavlo
 */
public class TestMarkovVertex extends BaseTestCase {

    private static final int NUM_PARTITIONS = 5;
    Workload g;
    MarkovGraph graph;
    MarkovVertex commit;
    MarkovVertex abort;
    MarkovVertex start;
    
    final Integer[][] partitions = {
            { 2, 4 },
            { 3, 4 },
            { 0, 2 },
            { 1, 4 },
            { 0, 1, 2 },
            { 0 },
            { 0 },
            { 0 }
    };
    final boolean[] readonly = new boolean[] {
            false,
            true,
            false,
            true,
            true,
            true,
            false,
            true,
    };
    final MarkovVertex[] vertices = new MarkovVertex[this.partitions.length];

    private HashSet<MarkovVertex> stopSet;

    @Before
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(NUM_PARTITIONS);
        assertEquals(this.partitions.length, this.readonly.length);
        
        Procedure catalog_proc = this.getProcedure("InsertCallForwarding");
        Map<Statement, AtomicInteger> instance_ctr = new HashMap<Statement, AtomicInteger>();
        for (int i = 0; i < this.partitions.length; i++) {
            Statement catalog_stmt = catalog_proc.getStatements().get(this.readonly[i] ? "query2" : "update");
            assertNotNull(catalog_stmt);
            if (!instance_ctr.containsKey(catalog_stmt)) {
                instance_ctr.put(catalog_stmt, new AtomicInteger(0));
            }
            this.vertices[i] = new MarkovVertex(catalog_stmt,
                                                MarkovVertex.Type.QUERY,
                                                instance_ctr.get(catalog_stmt).getAndIncrement(),
                                                new PartitionSet(this.partitions[i]),
                                                new PartitionSet());
        } // FOR
        
        graph = new MarkovGraph(catalog_proc);
        graph.initialize();
        start = graph.getStartVertex();
        commit = graph.getCommitVertex();
        abort = graph.getAbortVertex();

        for (MarkovVertex v : vertices) {
            graph.addVertex(v);
        }
        for (int i = 0; i < 10; i++) {
            graph.addToEdge(vertices[4], vertices[7]);
            graph.addToEdge(vertices[6], abort);
        }
        for (int i = 0; i < 20; i++) {
            graph.addToEdge(vertices[4], abort);
        }
        for (int i = 0; i < 30; i++) {
            // ---------_TOP PART OF GRAPH_---------
            graph.addToEdge(start, vertices[0]);
            graph.addToEdge(vertices[0], vertices[1]);
            graph.addToEdge(vertices[1], vertices[2]);
            graph.addToEdge(vertices[2], commit);
            // ---------_MIDDLE WAY AND BOTTOM_--------------
            graph.addToEdge(start, vertices[3]);
            graph.addToEdge(vertices[3], vertices[4]);
            graph.addToEdge(vertices[6], vertices[7]);
        }
        for (int i = 0; i < 40; i++) {
            graph.addToEdge(start, vertices[5]);
            graph.addToEdge(vertices[5], vertices[6]);
            graph.addToEdge(vertices[7], commit);
        }
        graph.calculateProbabilities(catalogContext.getAllPartitionIds());
        // assert(graph.isSane());
        
        // System.out.println(GraphvizExport.export(graph, "markov"));
        
        stopSet = new HashSet<MarkovVertex>();
        stopSet.add(graph.getCommitVertex());

    }

    // ----------------------------------------------------------------------------
    // TEST METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testIsEqual
     */
    @Test
    public void testIsEqual() throws Exception {
        MarkovVertex v0 = this.vertices[0];
        assertNotNull(v0);
        v0.getPastPartitions().add(1);
        
        MarkovVertex v1 = this.vertices[1];
        assertNotNull(v1);
        v1.getPastPartitions().add(2);
        
        
        // Test to make sure that Vertex.isEqual() works with and without the PAST_PARTITIONS flag
        for (int i = 0; i <= 1; i++) {
            Statement catalog_stmt = this.vertices[i].getCatalogItem();
            PartitionSet partitions = this.vertices[i].getPartitions();
            PartitionSet past_partitions = this.vertices[i].getPastPartitions();
            int query_index = this.vertices[i].getQueryCounter();
            
            assertNotNull(catalog_stmt);
            assertNotNull(partitions);
            assertFalse(partitions.isEmpty());
            assertNotNull(past_partitions);
            assertFalse(past_partitions.isEmpty());
            assert(query_index >= 0);

            MarkovVertex new_v = new MarkovVertex((Statement)this.vertices[i].getCatalogItem(),
                    this.vertices[i].getType(),
                    this.vertices[i].getQueryCounter(),
                    new PartitionSet(this.vertices[i].getPartitions()),
                    PartitionSet.singleton(1));
            for (boolean past_partitions_flag : new boolean[]{false, true}) {
                // If we're at the first vertex, then isEqual() should always return true regardless
                // of whether we are using the past_partitions_flag or not
                // Otherwise, we should only return true if the past_partitions_flag is false
                boolean expected = (i == 0 || past_partitions_flag == false);
                boolean result = new_v.isEqual(catalog_stmt, partitions, past_partitions, query_index, past_partitions_flag);
                if (expected != result) {
                    System.err.println(new_v);
                    System.err.println(this.vertices[i]);
                }
                assertEquals("[" + i + "] past_partitions_flag = " + past_partitions_flag,
                             expected, result);
            }
        } // FOR
    }
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        Set<String> base_keys = new HashSet<String>();
        for (AbstractVertex.Members m : AbstractVertex.Members.values()) {
            base_keys.add(m.name());
        } // FOR
        
        for (MarkovVertex v : vertices) {
            String json = v.toJSONString();
            Set<String> expected_keys = new HashSet<String>();
            for (MarkovVertex.Members m : MarkovVertex.Members.values()) {
                assert(json.contains(m.name()));
                expected_keys.add(m.name());
            } // FOR
            expected_keys.addAll(base_keys);
            expected_keys.add("ELEMENT_ID");
            
            JSONObject json_obj = new JSONObject(json);
            assertNotNull(json_obj);
            Iterator<String> keys = json_obj.keys();
            int count = 0;
            while (keys.hasNext()) {
                String key = keys.next();
                assert(expected_keys.contains(key)) : "Unexpected JSON key '" + key + "'";
                count++;
            } // WHILE
            assertEquals(expected_keys.size(), count);
        } // FOR
    }

    /**
     * testResetAllProbabilities
     */
    @Test
    public void testResetAllProbabilities() {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        Statement catalog_stmt = this.getStatement(catalog_proc, "GetData");
        MarkovVertex v = new MarkovVertex(catalog_stmt,
                                          Type.QUERY,
                                          0,
                                          catalogContext.getAllPartitionIds(),
                                          new PartitionSet());
        
        
        // System.err.println(start.debug());
        
        assertNotNull(start);
        v.setAbortProbability(0.50f);
//        v.setSinglePartitionProbability(0.50f);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            v.setDoneProbability(i, 0.50f);
            v.setWriteProbability(i, 0.50f);
//            v.setReadOnlyProbability(i, 0.50f);
        } // FOR
        
        v.resetAllProbabilities();
        
        assertEquals(0.0f, v.getAbortProbability());
//        assertEquals(0.0f, v.getSinglePartitionProbability());
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assertEquals(1.0f, v.getDoneProbability(i));
            assertEquals(0.0f, v.getWriteProbability(i));
//            assertEquals(0.0f, v.getReadOnlyProbability(i));
        } // FOR
    }

    /**
     * testCalculateAbortProbability
     */
//    @Test
//    public void testCalculateAbortProbability() {
//        Set<Vertex> vs = new HashSet<Vertex>();
//        vs.add(graph.getAbortVertex());
//        Vertex.calculateAbortProbability(vs, graph);
//        roughlyEqual(start.getAbortProbability(), 0.3);
//        double[] probs = { 0.0, 0.0, 0.0, 2.0 / 3.0, 2.0 / 3.0, 0.25, 0.25, 0.0 };
//        for (int i = 0; i < vertices.length; i++) {
//            roughlyEqual(vertices[i].getAbortProbability(), probs[i]);
//        }
//    }

    /**
     * testCalculateSingleSitedProbability
     */
//    @Test
//    public void testCalculateSingleSitedProbability() {
//        Vertex.calculateSingleSitedProbability(stopSet, graph);
//        roughlyEqual(start.getSingleSitedProbability(), 0.4);
//        double[] probs = { 0.0, 0.0, 0.0, 0.0, 1.0 / 3.0, 1.0, 0.75, 0.0 };
//        for (int i = 0; i < vertices.length; i++) {
//            roughlyEqual(vertices[i].getSingleSitedProbability(), probs[i]);
//        }
//    }

    /**
     * testCalculateReadOnlyProbability
     */
//    @Test
//    public void testCalculateReadOnlyProbability() {
//        Vertex.calculateReadOnlyProbability(stopSet, graph);
//        final double[] startreadprobs = {
//                0.3 + 0.3, // ?? 0.4 + 0.3,
//                0.3 + 0.3,
//                0.3,
//                1.0,
//                0.3
//        };
//        final double[] startwriteprobs = {
//                0.3, // 0.7
//                0.3,
//                0.0,
//                0.3,
//                0.0
//        };
//        assertEquals(startreadprobs.length, startwriteprobs.length);
//        
//        // System.err.println(start.debug());
//        
//        assertNotNull(start);
//        List<Integer> partitions = CatalogUtil.getAllPartitions(catalog_db);
//        for (int i = 0; i < startreadprobs.length; i++) {
//            int partition_id = partitions.get(i);
//            assertNotNull(startreadprobs[i]);
////            System.err.println("[" + i + "] " + start.getReadOnlyProbability(partition_id) + " --> " + startreadprobs[i]); 
//           // roughlyEqual(start.getReadOnlyProbability(partition_id), startreadprobs[i]);
////            System.err.println("[" + i + "] " + start.getWriteProbability(partition_id) + " --> " + startwriteprobs[i]);
////            roughlyEqual(start.getWriteProbability(partition_id), startwriteprobs[i]);
////            System.err.println();
//        } // FOR
//        
//        
//        double[][] readprobs = {
//                { 1.0, 0.0, 1.0, 1.0, 1.0 }, // Vertex 0---
//                { 1.0, 0.0, 1.0, 1.0, 1.0 }, // Vertex 1
//                { 1.0, 0.0, 1.0, 0.0, 0.0 }, // Vertex 2
//                { 1.0, 1.0, 1.0, 0.0, 1.0 }, // Vertex 3---
//                { 1.0, 1.0, 1.0, 0.0, 0.0 }, // Vertex 4
//                { 1.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 5---
//                { 1.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 6
//                { 1.0, 0.0, 0.0, 0.0, 0.0 }  // Vertex 7
//        };
//        double[][] writeprobs = {
//                { 1.0, 0.0, 1.0, 0.0, 1.0 }, // Vertex 0
//                { 1.0, 0.0, 1.0, 0.0, 0.0 }, // Vertex 1
//                { 1.0, 0.0, 1.0, 0.0, 0.0 }, // Vertex 2
//                { 0.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 3
//                { 0.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 4
//                { 1.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 5
//                { 1.0, 0.0, 0.0, 0.0, 0.0 }, // Vertex 6
//                { 0.0, 0.0, 0.0, 0.0, 0.0 }  // Vertex 7
//        };
//        for (int i = 0; i < vertices.length; i++) {
//            for (int partition : partitions) {
//                /* FIXME(svelagap)
////                System.err.println("[" + i + ", " + partition + "]");
//                roughlyEqual(vertices[i].getReadOnlyProbability(partition), readprobs[i][partition]);
//                roughlyEqual(vertices[i].getWriteProbability(partition), writeprobs[i][partition]);
//                */
//            } // FOR
//        } // FOR
//    }


//    /**
//     * testCalculateDoneProbability
//     */
//    @Test
//    public void testCalculateDoneProbability() {
//        Vertex.calculateDoneProbability(graph.getCommitVertex(), graph);
//        double[][] doneprobs = {
//                { 0.0, 1.0, 0.0, 0.0, 0.0 }, // Vertex 0
//                { 0.0, 1.0, 0.0, 0.0, 0.0 }, // Vertex 1
//                { 0.0, 1.0, 0.0, 1.0, 1.0 }, // Vertex 2
//                { 0.0, 0.0, 0.0, 1.0, 0.0 }, // Vertex 3
//                { 0.0, 0.0, 0.0, 1.0, 1.0 }, // Vertex 4
//                { 0.0, 1.0, 1.0, 1.0, 1.0 }, // Vertex 5
//                { 0.0, 1.0, 1.0, 1.0, 1.0 }, // Vertex 6
//                { 0.0, 1.0, 1.0, 1.0, 1.0 }  // Vertex 7
//        };
//        List<Integer> partitions = CatalogUtil.getAllPartitions(catalog_db);
//        for (int i = 0; i < vertices.length; i++) {
//            // System.err.println(vertices[i].debug());
//            for (int partition : partitions) {
//                // System.err.println("[" + i + ", " + partition + "] " + vertices[i].getDoneProbability(partition));
//                // FIXME(svelagap) roughlyEqual(vertices[i].getDoneProbability(partition), doneprobs[i][partition]);
//            } // FOR
//        } // FOR
//    }
}