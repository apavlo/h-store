package edu.brown.graphs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import org.junit.Test;

import org.voltdb.catalog.CatalogType;

import edu.brown.graphs.VertexTreeWalker.TraverseOrder;
import edu.brown.utils.ArgumentsParser;

/**
 * @author pavlo
 */
public class TestVertexTreeWalker extends TestCase {
    
    public class MockVertex extends AbstractVertex {
        public final Integer id;
        public MockVertex(int id) {
            this.id = id;
        }
        @Override
        public <T extends CatalogType> T getCatalogItem() {
            return (null);
        }
        @Override
        public int hashCode() {
            return (this.id.hashCode());
        }
        @Override
        public String toString() {
            return "[" + id.toString() + "]";
        }
    } // CLASS

    public class MockEdge extends AbstractEdge {
        public final Integer id;
        public MockEdge(int id) {
            super(null);
            this.id = id;
        }
        @Override
        public int hashCode() {
            return (this.id.hashCode());
        }
        @Override
        public String toString() {
            return "[" + id.toString() + "]";
        }
    } // CLASS

    private AbstractDirectedGraph<MockVertex, MockEdge> graph;
    private MockVertex vertices[] = new MockVertex[7];
    
    @Override
    protected void setUp() throws Exception {
        ArgumentsParser.setupLogging();
        
        // Create a graph that looks like the following:
        //
        //              [0]
        //             /   \
        //          [1]     [2]
        //         /   \     |
        //      [3]     [4] [5]
        //       |
        //      [6]
        //
        this.graph = new AbstractDirectedGraph<MockVertex, MockEdge>(null) {
            private static final long serialVersionUID = 1L;
        };
        
        for (int i = 0; i < vertices.length; i++) {
            this.vertices[i] = new MockVertex(i);
            this.graph.addVertex(this.vertices[i]);
        } // FOR

        int edge_id = 1;
        this.graph.addEdge(new MockEdge(edge_id++), vertices[0], vertices[1]);
        this.graph.addEdge(new MockEdge(edge_id++), vertices[0], vertices[2]);
        
        this.graph.addEdge(new MockEdge(edge_id++), vertices[1], vertices[3]);
        this.graph.addEdge(new MockEdge(edge_id++), vertices[1], vertices[4]);
        
        this.graph.addEdge(new MockEdge(edge_id++), vertices[2], vertices[5]);
        
        this.graph.addEdge(new MockEdge(edge_id++), vertices[3], vertices[6]);
    }
    
    /**
     * testBreadthFirstSearch
     */
    @Test
    public void testBreadthFirstSearch() {
        final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4, 5, 6);
        assertEquals(this.vertices.length, expected.size());
        
        final List<Integer> result = new ArrayList<Integer>(); 
        new VertexTreeWalker<MockVertex>(this.graph, TraverseOrder.BREADTH) {
            @Override
            protected void callback(MockVertex element) {
                result.add(element.id);
            }
        }.traverse(this.vertices[0]);
        
        System.err.println("Expected: " + expected);
        System.err.println("Result: " + result);
        
        assertEquals(expected.size(), result.size());
        for (int i = 0, cnt = expected.size(); i < cnt; i++) {
            assertEquals("[" + i + "]", expected.get(i), result.get(i));
        } // FOR
    }
    
    /**
     * testDepthFirstSearch
     */
    @Test
    public void testDepthFirstSearch() {
        final List<Integer> expected = Arrays.asList(0, 1, 3, 6, 4, 2, 5);
        assertEquals(this.vertices.length, expected.size());
        
        final List<Integer> result = new ArrayList<Integer>(); 
        new VertexTreeWalker<MockVertex>(this.graph, TraverseOrder.DEPTH) {
            @Override
            protected void callback(MockVertex element) {
                result.add(element.id);
            }
        }.traverse(this.vertices[0]);
        
        System.err.println("Expected: " + expected);
        System.err.println("Result: " + result);
        
        assertEquals(expected.size(), result.size());
        for (int i = 0, cnt = expected.size(); i < cnt; i++) {
            assertEquals("[" + i + "]", expected.get(i), result.get(i));
        } // FOR
    }
}
