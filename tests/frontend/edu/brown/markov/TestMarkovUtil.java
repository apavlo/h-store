package edu.brown.markov;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONObject;
import org.voltdb.catalog.Procedure;

import edu.brown.BaseTestCase;
import edu.brown.benchmark.tm1.procedures.GetNewDestination;
import edu.brown.markov.containers.MarkovGraphsContainerUtil;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

public class TestMarkovUtil extends BaseTestCase {
    
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
        this.addPartitions(10);
    }
    
    private void examineVertices(MarkovVertex v0, MarkovVertex v1) {
        assertNotNull(v0);
        assertNotNull(v1);
        assertNotSame(v0, v1);
        assertEquals(v0.getCatalogKey(), v0.getCatalogKey());
        assertEquals(v0.getCatalogItem(), v1.getCatalogItem());
    }
    
    /**
     * testSerialization
     */
    public void testSerialization() throws Exception {
        Procedure catalog_proc = this.getProcedure(GetNewDestination.class);
        
        // Make a bunch of MarkovGraphsContainers
        Map<Integer, MarkovGraphsContainer> markovs = new HashMap<Integer, MarkovGraphsContainer>();
        for (int i = 1000; i < 1010; i++) {
            MarkovGraphsContainer m = new MarkovGraphsContainer();
            for (int p : catalogContext.getAllPartitionIds()) {
                m.getOrCreate(p, catalog_proc).initialize();
            } // FOR
            markovs.put(i, m);
        } // FOR
        
        // Serialize them out to a file. This will also make a nice little index in the file
        File temp = FileUtil.getTempFile("markovs", true);
        assertNotNull(temp);
        MarkovGraphsContainerUtil.save(markovs, temp);
//        System.err.println("MARKOV FILE: " + temp);
        
        // Now read it back in make sure everything is there
        Map<Integer, MarkovGraphsContainer> clone = MarkovUtil.load(catalogContext, temp);
        assertNotNull(clone);
        assertEquals(markovs.size(), clone.size());
        assert(markovs.keySet().containsAll(clone.keySet()));
        for (Integer id : markovs.keySet()) {
            MarkovGraphsContainer clone_m = clone.get(id);
            assertNotNull(clone_m);
        } // FOR
    }
    
    /**
     * testGetStartVertex
     */
    public void testGetStartVertex() throws Exception {
        this.examineVertices(MarkovUtil.getStartVertex(catalogContext), MarkovUtil.getStartVertex(catalogContext)); 
    }

    /**
     * testGetStopVertex
     */
    public void testGetStopVertex() throws Exception {
        this.examineVertices(MarkovUtil.getCommitVertex(catalogContext), MarkovUtil.getCommitVertex(catalogContext)); 
    }

    /**
     * testGetAbortVertex
     */
    public void testGetAbortVertex() throws Exception {
        this.examineVertices(MarkovUtil.getAbortVertex(catalogContext), MarkovUtil.getAbortVertex(catalogContext)); 
    }

    /**
     * testSpecialVertexSerialization
     */
    public void testSpecialVertexSerialization() throws Exception {
        MarkovVertex.Type types[] = new MarkovVertex.Type[] {
                MarkovVertex.Type.START,
                MarkovVertex.Type.COMMIT,
                MarkovVertex.Type.ABORT,
        };
        for (MarkovVertex.Type type : types) {
            MarkovVertex v = MarkovUtil.getSpecialVertex(catalogContext.database, type);
            assertNotNull(v);
            
            String json = v.toJSONString();
            assertFalse(json.isEmpty());
            assertTrue(json.contains(v.getCatalogKey()));
        } // FOR
    }
    
    /**
     * testVertexDeserialization
     */
    public void testSpecialVertexDeserialization() throws Exception {
        MarkovVertex.Type types[] = new MarkovVertex.Type[] {
                MarkovVertex.Type.START,
                MarkovVertex.Type.COMMIT,
                MarkovVertex.Type.ABORT,
        };
        for (MarkovVertex.Type type : types) {
            MarkovVertex v = MarkovUtil.getSpecialVertex(catalogContext.database, type);
            assertNotNull(v);
            
            String json = v.toJSONString();
            JSONObject json_obj = new JSONObject(json);
            assertNotNull(json_obj);
            //System.err.println(json_obj.toString(2));
            
            MarkovVertex clone = new MarkovVertex();
            clone.fromJSON(json_obj, catalogContext.database);
            this.examineVertices(v, clone);
            
//            System.err.println(clone.getCatalogItem());
//            System.err.println(clone.getCatalogItem().getClass());
//            System.err.println("--------------------");
        } // FOR
    }
}
