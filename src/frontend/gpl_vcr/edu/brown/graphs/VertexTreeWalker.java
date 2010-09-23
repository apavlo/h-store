/**
 * 
 */
package edu.brown.graphs;

import edu.brown.utils.AbstractTreeWalker;

/**
 * @author pavlo
 *
 */
public abstract class VertexTreeWalker<V extends AbstractVertex> extends AbstractTreeWalker<V> {
    public enum TraverseOrder {
        BREADTH,
        DEPTH,
        LONGEST_PATH,
    };
    
    private final IGraph<V, ? extends AbstractEdge> graph;
    private final TraverseOrder order;
    
    public VertexTreeWalker(IGraph<V, ? extends AbstractEdge> graph, TraverseOrder order) {
        this.graph = graph;
        this.order = order;
    }
    
    public VertexTreeWalker(IGraph<V, ? extends AbstractEdge> graph) {
        this(graph, TraverseOrder.DEPTH);
    }
    
    public final IGraph<V, ? extends AbstractEdge> getGraph() {
        return this.graph;
    }
    
    /* (non-Javadoc)
     * @see org.voltdb.utils.AbstractTreeWalker#traverse_children(java.lang.Object)
     */
    @Override
    protected void populate_children(VertexTreeWalker<V>.Children children, V element) {
        for (V child : this.graph.getSuccessors(element)) {
            if (!this.hasVisited(child)) {
                switch (this.order) {
                    case DEPTH:
                        children.addBefore(child);
                        break;
                    case BREADTH:
                        children.addAfter(child);
                        break;
                    case LONGEST_PATH: {
                        // Check whether the parents of this vertex have already been visited
                        // If they have, then yeah we'll go ahead and visit it now
                        boolean all_visited = true;
                        for (V parent : this.graph.getPredecessors(child)) {
                            if (!element.equals(parent) && !this.hasVisited(parent)) {
                                all_visited = false;
                                break;
                            }
                        } // FOR
                        if (all_visited) children.addAfter(child);
                        break;
                    }
                    default:
                        assert(false) : "Unimplemented TraverseOrder: " + this.order;
                } // SWITCH
            }
        } // FOR
        return;
    }
}