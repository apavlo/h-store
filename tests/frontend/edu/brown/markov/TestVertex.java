package edu.brown.markov;

import java.io.File;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.catalog.CatalogUtil;
import edu.brown.graphs.AbstractVertex;
import edu.brown.graphs.GraphvizExport;
import edu.brown.markov.Vertex.Type;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;
import edu.brown.workload.AbstractWorkload;

public class TestVertex extends BaseTestCase {

    private static final int NUM_PARTITIONS = 5;
    private static final double EPSILON = 0.00001;
    AbstractWorkload g;
    MarkovGraph graph;
    Vertex commit;
    Vertex abort;
    Vertex start;
    
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
    final Vertex[] vertices = new Vertex[this.partitions.length];

    private HashSet<Vertex> stopSet;

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
            this.vertices[i] = new Vertex(catalog_stmt, Vertex.Type.QUERY, (long)instance_ctr.get(catalog_stmt).getAndIncrement(), Arrays.asList(this.partitions[i]));
        } // FOR
        
        graph = new MarkovGraph(catalog_proc, 0);
        graph.initialize();
        start = graph.getStartVertex();
        commit = graph.getCommitVertex();
        abort = graph.getAbortVertex();

        for (Vertex v : vertices) {
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
        graph.calculateEdgeProbabilities();
        // assert(graph.isSane());
        
        // System.out.println(GraphvizExport.export(graph, "markov"));
        
        stopSet = new HashSet<Vertex>();
        stopSet.add(graph.getCommitVertex());

    }

    private void roughlyEqual(double prob, double d) {
        double diff = Math.abs(prob - d);
        assert(diff <= EPSILON) : "Difference between " + prob + " and " + d + " is " + diff + ". " +
                                  "Must be at most " + String.format("%.5f", EPSILON);
    }
    
    // ----------------------------------------------------------------------------
    // TEST METHODS
    // ----------------------------------------------------------------------------
    
    /**
     * testToJSONString
     */
    public void testToJSONString() throws Exception {
        Set<String> base_keys = new HashSet<String>();
        for (AbstractVertex.Members m : AbstractVertex.Members.values()) {
            base_keys.add(m.name());
        } // FOR
        
        for (Vertex v : vertices) {
            String json = v.toJSONString();
            Set<String> expected_keys = new HashSet<String>();
            for (Vertex.Members m : Vertex.Members.values()) {
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
        Vertex v = new Vertex(catalog_stmt, Type.QUERY, 0, CatalogUtil.getAllPartitions(catalog_stmt));
        
        v.setAbortProbability(0.50d);
        v.setSingleSitedProbability(0.50d);
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            v.setDoneProbability(i, 0.50d);
            v.setWriteProbability(i, 0.50d);
            v.setReadOnlyProbability(i, 0.50d);
        } // FOR
        
        v.resetAllProbabilities();
        
        assertEquals(0.0d, v.getAbortProbability());
        assertEquals(0.0d, v.getSingleSitedProbability());
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            assertEquals(1.0d, v.getDoneProbability(i));
            assertEquals(0.0d, v.getWriteProbability(i));
            assertEquals(0.0d, v.getReadOnlyProbability(i));
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