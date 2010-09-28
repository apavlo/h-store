package edu.brown.utils;

import java.util.*;

public abstract class AbstractTreeWalker<E> {
    
    public class Children {
        private final E parent;
        private final List<E> before_list = new ArrayList<E>();
        private final List<E> after_list = new ArrayList<E>();
        
        public Children(E parent) {
            this.parent = parent;
        }

        public List<E> getBefore() {
            return (this.before_list);
        }
        public void addBefore(E child) {
            if (child != null) this.before_list.add(child);
        }
        public void addBefore(Collection<E> children) {
            if (children != null) {
                for (E child : children) {
                    if (child != null) this.before_list.add(child);
                } // FOR
            }
        }
        public void addBefore(E children[]) {
            for (E child : children) {
                if (child != null) this.before_list.add(child);
            } // FOR
        }
        
        public List<E> getAfter() {
            return (this.after_list);
        }
        public void addAfter(E child) {
            this.after_list.add(child);
        }
        public void addAfter(Collection<E> children) {
            for (E child : children) {
                if (child != null) this.after_list.add(child);
            } // FOR
        }
        public void addAfter(E children[]) {
            if (children != null) {
                for (E child : children) {
                    if (child != null) this.after_list.add(child);
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
     * How many nodes we have visited
     */
    private int counter = 0;
    
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
     * Returns the ordered list of elements that were visited 
     * @return
     */
    public List<E> getVisitPath() {
        return (Collections.unmodifiableList(this.visited));
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
     * The callback() method can call this if it wants the walker to break out of the traversal
     */
    protected final void stop() {
        this.stop = true;
    }

    // ----------------------------------------------------------------------
    // TRAVERSAL METHODS
    // ----------------------------------------------------------------------
    
    /**
     * Depth first traversal
     * @param element
     */
    @SuppressWarnings("unchecked")
    public final void traverse(E element) {
        assert(element != null) : "AbstractTreeWalker.traverse() was passed a null element";
        if (this.first == null) this.first = element;
        if (this.stop) return;
        if (this.counter == 0) this.callback_first(element);
        this.stack.push(element);
        this.depth++;
        this.counter++;
    
        if (depth > 150) {
            System.err.println("Stack:\n" + StringUtil.join("\n", this.stack));
            throw new RuntimeException("I think we've gone too far: " + element + " [parent=" + this.getPrevious() + "]");
        }
        
        this.callback_before(element);
        
        // Get the list of children to visit before and after we call ourself
//        if (!this.visited.contains(element)) {
            AbstractTreeWalker<E>.Children children = new AbstractTreeWalker.Children(element);
            this.populate_children(children, element);
            for (E child : children.before_list) {
                this.traverse(child);
            }
            
            this.visited.add(element);
            this.callback(element);

            for (E child : children.after_list) {
                this.traverse(child);
            }
//        }
        
        E check_exp = this.stack.pop();
        assert(element.equals(check_exp));
        
        this.callback_after(element);
        this.depth--;
        if (this.depth == -1) this.callback_last(element);
        return;
    }
    /**
     * 
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
     * Callback method before we visit any of the node's children
     * @param element
     */
    protected void callback_before(E element) {
        // Do nothing
    }
    /**
     * Callback method after we have finished visiting all of a node's children
     * and will return back to their parent
     * @param element
     */
    protected void callback_after(E element) {
        // Do nothing
    }
    /**
     * Callback method on the very first element in the tree
     * @param element
     */
    protected void callback_first(E element) {
        // Do nothing
    }
    /**
     * Callback method on the very last element in the tree
     * @param element
     */
    protected void callback_last(E element) {
        // Do nothing
    }
}
