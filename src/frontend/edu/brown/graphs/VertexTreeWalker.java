package edu.brown.graphs;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;

import edu.brown.utils.AbstractTreeWalker;
import edu.brown.utils.CollectionUtil;

/**
 * @author pavlo
 *
 */
public abstract class VertexTreeWalker<V extends AbstractVertex, E extends AbstractEdge> extends AbstractTreeWalker<V> {
    private static final Logger LOG = Logger.getLogger(VertexTreeWalker.class);
    
    public enum TraverseOrder {
        /** Breadth-First Search */
        BREADTH,
        /** Depth-First Search */
        DEPTH,
        /** 
         * Traverse the graph such that the first callback is for the first element that
         * is farthest away from the given root
         */
        LONGEST_PATH,
    };
    public enum Direction {
        /** Traverse the DirectedGraph from parent to child */
        FORWARD,
        /** Traverse the DirectedGraph from child to parent */
        REVERSE,
        /** Traverse the UndirectedGraph in any direction */
        ANY;
        
        public Direction getReverse() {
            switch (this) {
                case FORWARD:
                    return (REVERSE);
                case REVERSE:
                    return (FORWARD);
                case ANY:
                    return (ANY);
            }
            return (null);
        }
    };
    
    
    private IGraph<V, E> graph;
    private TraverseOrder search_order;
    private Direction search_direction; 

    /**
     * Breadth-First Search
     * The last element at each depth in the tree. This is where we will still all of the
     * children that we need to visit in the next level
     */
    private Map<Integer, V> bfs_levels;

    protected VertexTreeWalker() {
        // Nothing!
    }
    public VertexTreeWalker(IGraph<V, E> graph, TraverseOrder order, Direction direction) {
        this.init(graph, order, direction);
    }
    public VertexTreeWalker(IGraph<V, E> graph) {
        this(graph, TraverseOrder.DEPTH, Direction.FORWARD);
    }
    public VertexTreeWalker(IGraph<V, E> graph, TraverseOrder order) {
        this(graph, order, Direction.FORWARD);
    }

    public VertexTreeWalker<V, E> init(IGraph<V, E> graph, TraverseOrder order, Direction direction) {
        this.graph = graph;
        this.search_order = order;
        this.search_direction = direction;
        
        if (this.search_order == TraverseOrder.BREADTH && this.bfs_levels == null) {
            this.bfs_levels = new HashMap<Integer, V>();
        }
        return (this);
    }
    
    @Override
    public void finish() {
        super.finish();
        if (this.bfs_levels != null) this.bfs_levels.clear();
    }
    
    public final IGraph<V, E> getGraph() {
        return this.graph;
    }
    
    protected Collection<V> getNext(Direction direction, V element) {
        Collection<V> next = null;
        switch (direction) {
            case FORWARD:
                next = this.graph.getSuccessors(element);
                break;
            case REVERSE:
                next = this.graph.getPredecessors(element);
                break;
            case ANY:
                next = this.graph.getNeighbors(element);
                break;
            default:
                assert(false) : "Unexpected search direction: " + direction;
        } // SWITCH
        return (next);
    }
    
    /* (non-Javadoc)
     * @see org.voltdb.utils.AbstractTreeWalker#traverse_children(java.lang.Object)
     */
    @Override
    protected void populate_children(VertexTreeWalker.Children<V> children, V element) {
        final boolean trace = LOG.isTraceEnabled();
        ListOrderedSet<V> bfs_children = null;
        
        if (trace) LOG.trace("Populating Children for " + element + " [direction=" + this.search_direction + "]"); 
        
        for (V child : this.getNext(this.search_direction, element)) {
            if (!this.hasVisited(child)) {
                switch (this.search_order) {
                    case DEPTH:
                        children.addAfter(child);
                        break;
                    case BREADTH:
                        if (bfs_children == null) bfs_children = new ListOrderedSet<V>();
                        bfs_children.add(child);
                        break;
                    case LONGEST_PATH: {
                        // Check whether the parents of this vertex have already been visited
                        // If they have, then yeah we'll go ahead and visit it now
                        boolean all_visited = true;
                        for (V parent : this.getNext(this.search_direction.getReverse(), child)) {
                            if (!element.equals(parent) && !this.hasVisited(parent)) {
                                all_visited = false;
                                break;
                            }
                        } // FOR
                        if (all_visited) children.addAfter(child);
                        break;
                    }
                    default:
                        assert(false) : "Unimplemented TraverseOrder: " + this.search_order;
                } // SWITCH
            }
        } // FOR
        
        // Special Case: Breadth-First Search
        if (this.search_order == TraverseOrder.BREADTH && bfs_children != null && bfs_children.isEmpty() == false) {
            if (element.equals(this.getFirst())) {
                children.addAfter(bfs_children);
                
            // Otherwise we need attach all of the children to the last element at this depth
            // We want to make sure that if there is already a last element that we remove all of their
            // children and put it into our new last element
            } else {
                V last_element = this.bfs_levels.get(this.getDepth());
                assert(last_element != null) : "Null last_element at depth " + this.getDepth();

                // Put all of our children into the last element's in our depth children
                Children<V> last_element_c = this.getChildren(last_element);
                for (V child : bfs_children) {
                    // We have to do this to avoid duplicates...
                    if (last_element_c.getAfter().contains(child) == false) last_element_c.addAfter(child);
                } // FOR
                
                if (trace) LOG.trace("BFS Last Element [depth=" + this.getDepth() + "]: " + last_element + " => " + last_element_c.getAfter());
            }
        
            // Always mark our last child as the next child in the next depth
            this.bfs_levels.put(this.getDepth() + 1, CollectionUtil.last(bfs_children.asList()));
        }
        
        return;
    }

}