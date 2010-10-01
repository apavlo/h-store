package edu.brown.markov;

import org.json.JSONObject;

import edu.brown.BaseTestCase;
import edu.brown.utils.ProjectType;

public class TestMarkovUtil extends BaseTestCase {
    
    public void setUp() throws Exception {
        super.setUp(ProjectType.TM1);
    }
    
    private void examineVertices(Vertex v0, Vertex v1) {
        assertNotNull(v0);
        assertNotNull(v1);
        assertNotSame(v0, v1);
        assertEquals(v0.getCatalogKey(), v0.getCatalogKey());
        assertEquals(v0.getCatalogItem(), v1.getCatalogItem());
    }
    
    /**
     * testGetStartVertex
     */
    public void testGetStartVertex() throws Exception {
        this.examineVertices(MarkovUtil.getStartVertex(catalog_db), MarkovUtil.getStartVertex(catalog_db)); 
    }

    /**
     * testGetStopVertex
     */
    public void testGetStopVertex() throws Exception {
        this.examineVertices(MarkovUtil.getCommitVertex(catalog_db), MarkovUtil.getCommitVertex(catalog_db)); 
    }

    /**
     * testGetAbortVertex
     */
    public void testGetAbortVertex() throws Exception {
        this.examineVertices(MarkovUtil.getAbortVertex(catalog_db), MarkovUtil.getAbortVertex(catalog_db)); 
    }
    
    /**
     * testSerialization
     */
    public void testVertexSerialization() throws Exception {
        Vertex.Type types[] = new Vertex.Type[] {
                Vertex.Type.START,
                Vertex.Type.COMMIT,
                Vertex.Type.ABORT,
        };
        for (Vertex.Type type : types) {
            Vertex v = MarkovUtil.getSpecialVertex(catalog_db, type);
            assertNotNull(v);
            
            String json = v.toJSONString();
            assertFalse(json.isEmpty());
            assertTrue(json.contains(v.getCatalogKey()));
        } // FOR
    }
    
    /**
     * testDeserialization
     */
    public void testVertexDeserialization() throws Exception {
        Vertex.Type types[] = new Vertex.Type[] {
                Vertex.Type.START,
                Vertex.Type.COMMIT,
                Vertex.Type.ABORT,
        };
        for (Vertex.Type type : types) {
            Vertex v = MarkovUtil.getSpecialVertex(catalog_db, type);
            assertNotNull(v);
            
            String json = v.toJSONString();
            JSONObject json_obj = new JSONObject(json);
            assertNotNull(json_obj);
            //System.err.println(json_obj.toString(2));
            
            Vertex clone = new Vertex();
            clone.fromJSON(json_obj, catalog_db);
            this.examineVertices(v, clone);
            
//            System.err.println(clone.getCatalogItem());
//            System.err.println(clone.getCatalogItem().getClass());
//            System.err.println("--------------------");
        } // FOR
    }
}
