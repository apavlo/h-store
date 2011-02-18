package edu.brown.utils;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

import org.apache.log4j.Logger;

/**
 * @author pavlo
 * @param <E> element type
 */
public abstract class AbstractTreeWalker<E> implements Poolable {
    protected static final Logger LOG = Logger.getLogger(AbstractTreeWalker.class);
    
    public class Children {
        private final E parent;
        private final Queue<E> before_list = new ConcurrentLinkedQueue<E>();
        private final Queue<E> after_list = new ConcurrentLinkedQueue<E>();
        
        private Children(E parent) {
            this.parent = parent;
        }

        public Queue<E> getBefore() {
            return (this.before_list);
        }
        public void addBefore(E child) {
            if (child != null) {
                // assert(child.equals(parent) == false) : "Trying to add " + parent + " as a child of itself";
                this.before_list.add(child);
            }
        }
        public void addBefore(Collection<E> children) {
            if (children != null) {
                for (E child : children) {
                    this.addBefore(child);
                } // FOR
            }
        }
        public void addBefore(E children[]) {
            for (E child : children) {
                this.addBefore(child);
            } // FOR
        }
        
        public Queue<E> getAfter() {
            return (this.after_list);
        }
        public void addAfter(E child) {
            if (child != null) {
                // assert(child.equals(parent) == false) : "Trying to add " + parent + " as a child of itself";
                this.after_list.add(child);
            }
        }
        public void addAfter(Collection<E> children) {
            for (E child : children) {
                this.addAfter(child);
            } // FOR
        }
        public void addAfter(E children[]) {
            if (children != null) {
                for (E child : children) {
                    this.addAfter(child);
                } // FOR
            }
        }
        
        @Override
        public String toString() {
            String ret = this.parent.toString() + ": ";
            ret += "BEFORE" + this.before_list + " ";
            ret += "AFTER" + this.after_list;
            return (ret);
        }
    }
    
    /**
     * Tree Element Stack
     */
    private final Stack<E> stack = new Stack<E>();
    /**
     * List of elements that we visited (in proper order)
     */
    private final Vector<E> visited = new Vector<E>();
    /**
     * The first element that started the traversal
     */
    private E first = null;
    /**
     * How deep we are in the tree
     */
    private int depth = -1;
    /**
     * Flag used to short circuit the traversal
     */
    private boolean stop = false;
    /**
     * If set to true, then we're allowed to revisit the same node
     */
    private boolean allow_revisit = false;
    /**
     * How many nodes we have visited
     */
    private int counter = 0;
    /**
     * Others can attach Children to elements out-of-band so that we can do other
     * types of traversal through the tree
     */
    private final Map<E, Children> attached_children = new HashMap<E, Children>();
    /**
     * Depth limit (for debugging)
     */
    private int depth_limit = -1;
    
    // ----------------------------------------------------------------------
    // CLEANUP
    // ----------------------------------------------------------------------
    
    @Override
    public void finish() {
        this.stack.clear();
        this.visited.clear();
        this.first = null;
        this.depth = -1;
        this.stop = false;
        this.allow_revisit = false;
        this.counter = 0;
        this.depth_limit = -1;
        this.attached_children.clear();
    }
    
    // ----------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------
    
    /**
     * Return a Children singleton for a specific element
     * This allows us to add children either before our element is invoked
     * @param element
     * @return
     */
    protected final Children getChildren(E element) {
        Children c = this.attached_children.get(element);
        if (c == null) {
            c = new Children(element);
            this.attached_children.put(element, c);
        }
        return (c);
    }
    /**
     * Returns the parent of the current node in callback()
     * @return
     */
    protected final E getPrevious() {
        E ret = null;
        int size = this.stack.size(); 
        if (size > 1) ret = this.stack.get(size - 2); 
        return (ret);
    }
    /**
     * Returns the depth of the current callback() invocation
     * @return
     */
    protected final int getDepth() {
        return (this.depth);
    }
    /**
     * Returns the current visitation stack for vertices
     * @return
     */
    protected final Stack<E> getStack() {
        return (this.stack);
    }
    /**
     * Returns true if the given element has already been visited
     * @param element
     * @return
     */
    protected boolean hasVisited(E element) {
        return (this.visited.contains(element));
    }
    /**
     * Mark the given element as having been already visited
     * @param element
     */
    protected void markAsVisited(E element) {
        this.visited.add(element);
    }
    /**
     * Returns the ordered list of elements that were visited 
     * @return
     */
    public List<E> getVisitPath() {
        return (Collections.unmodifiableList(this.visited));
    }
    /**
     * Toggle whether the walker is allowed to revisit a vertex more than one
     * @param flag
     */
    public void setAllowRevisit(boolean flag) {
        this.allow_revisit = flag;
    }
    /**
     * Returns the first element that initiated the traversal
     * @return
     */
    protected final E getFirst() {
        return (this.first);
    }
    /**
     * Returns the count for the number of nodes that we have visited 
     * @return
     */
    public final int getCounter() {
        return (this.counter);
    }
    /**
     * Set the depth limit. Once this reached we will dump out the stack
     * @param limit
     */
    protected final void setDepthLimit(int limit) {
        this.depth_limit = limit;
    }
    /**
     * The callback() method can call this if it wants the walker to break out of the traversal
     */
    protected final void stop() {
        this.stop = true;
    }
    /**
     * Returns true is this walker has been marked as stopped
     * @return
     */
    protected final boolean isStopped() {
        return (this.stop);
    }

    // ----------------------------------------------------------------------
    // TRAVERSAL METHODS
    // ----------------------------------------------------------------------
    
    /**
     * Depth first traversal
     * @param element
     */
    public final void traverse(E element) {
        final boolean trace = LOG.isTraceEnabled();
        assert(element != null) : "AbstractTreeWalker.traverse() was passed a null element";
        
        if (trace) LOG.trace("traverse(" + element + ")");
        if (this.stop) {
            if (trace) LOG.trace("Stop Called. Halting traversal.");
            return;
        }
        if (this.first == null) {
            assert(this.counter == 0) : "Unexpected counter value on first element [" + this.counter + "]";
            this.first = element;
            if (trace) LOG.trace("callback_first(" + element + ")");
            this.callback_first(element);
        }

        this.stack.push(element);
        this.depth++;
        this.counter++;
        if (trace) LOG.trace("[Stack=" + this.stack.size() + ", " +
                              "Depth=" + this.depth + ", " + 
                              "Counter=" + this.counter + ", " +
                              "Visited=" + this.visited.size() + "]");
    
        // Stackoverflow check
        if (this.depth_limit >= 0 && this.depth > this.depth_limit) {
            LOG.fatal("Reached depth limit [" + this.depth + "]");
            System.err.println(StringUtil.join("\n", Thread.currentThread().getStackTrace()));
            System.exit(1);
        }
        
        if (trace) LOG.trace("callback_before(" + element + ")");
        this.callback_before(element);
        
        // Get the list of children to visit before and after we call ourself
//        if (!this.visited.contains(element)) {
            AbstractTreeWalker<E>.Children children = this.getChildren(element);
            this.populate_children(children, element);
            if (trace) LOG.trace("Populate Children: " + children);
            for (E child : children.before_list) {
                if (this.allow_revisit || this.visited.contains(child) == false) {
                    if (trace) LOG.trace("Traversing child " + child + "' before " + element);
                    this.traverse(child);
                }
            } // FOR
            
            // Why is this here and not up above when we update the stack?
            this.visited.add(element);
            
            if (trace) LOG.trace("callback(" + element + ")");
            this.callback(element);

            for (E child : children.after_list) {
                if (this.allow_revisit || this.visited.contains(child) == false) {
                    if (trace) LOG.trace("Traversing child " + child + " after " + element);
                    this.traverse(child);
                }
            } // FOR
//        }
        
        E check_exp = this.stack.pop();
        assert(element.equals(check_exp));
        
        this.callback_after(element);
        this.depth--;
        if (this.depth == -1) {
            if (trace) LOG.trace("callback_last(" + element + ")");
            this.callback_last(element);
        }
        return;
    }
    
    /**
     * For the given element, populate the Children object with the next items to visit either
     * before or after the callback method is called for the element 
     * @param element
     */
    protected abstract void populate_children(AbstractTreeWalker<E>.Children children, E element);
    
    // ----------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------
    
    /**
     * This method will be called after the walker has explored a node's children
     * @param element the object to perform an operation on
     */
    protected abstract void callback(E element);
    
    /**
     * Optional callback method before we visit any of the node's children
     * @param element
     */
    protected void callback_before(E element) {
        // Do nothing
    }
    /**
     * Optional callback method after we have finished visiting all of a node's children
     * and will return back to their parent.
     * @param element
     */
    protected void callback_after(E element) {
        // Do nothing
    }
    /**
     * Optional callback method on the very first element in the tree.
     * @param element
     */
    protected void callback_first(E element) {
        // Do nothing
    }
    /**
     * Optional callback method on the very last element in the tree
     * @param element
     */
    protected void callback_last(E element) {
        // Do nothing
    }
}
