/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.pool.BasePoolableObjectFactory;
import org.apache.log4j.Logger;

import edu.brown.pools.FastObjectPool;
import edu.brown.pools.Poolable;

/**
 * Generic abstract class that can be used to build different traversal programs
 * for various types of tree/graph structures
 * @author pavlo
 * @param <E> element type
 */
public abstract class AbstractTreeWalker<E> implements Poolable {
    private static final Logger LOG = Logger.getLogger(AbstractTreeWalker.class);

    private static final Map<Class<?>, FastObjectPool<?>> CHILDREN_POOLS = new HashMap<Class<?>, FastObjectPool<?>>();

    /**
     * Children Object Pool Factory
     */
    private static class ChildrenFactory<E> extends BasePoolableObjectFactory {
        @Override
        public Object makeObject() throws Exception {
            Children<E> c = new Children<E>();
            return (c);
        }

        public void passivateObject(Object obj) throws Exception {
            Children<?> c = (Children<?>) obj;
            c.finish();
        };
    };

    /**
     * The children data structure keeps track of the elements that we need to
     * visit before and after each element in the tree
     */
    public static class Children<E> implements Poolable {
        private E parent;
        private final Queue<E> before_list = new LinkedList<E>();
        private final Queue<E> after_list = new LinkedList<E>();

        private Children() {
            // Nothing...
        }

        public void init(E parent) {
            this.parent = parent;
        }

        @Override
        public boolean isInitialized() {
            return (this.parent != null);
        }

        @Override
        public void finish() {
            this.parent = null;
            this.before_list.clear();
            this.after_list.clear();
        }

        public Queue<E> getBefore() {
            return (this.before_list);
        }

        public void addBefore(E child) {
            if (child != null) {
                // assert(child.equals(parent) == false) : "Trying to add " +
                // parent + " as a child of itself";
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
                // assert(child.equals(parent) == false) : "Trying to add " +
                // parent + " as a child of itself";
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
    private final List<E> visited = new ArrayList<E>();
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
     * Others can attach Children to elements out-of-band so that we can do
     * other types of traversal through the tree
     */
    private final Map<E, Children<E>> attached_children = new HashMap<E, Children<E>>();
    /**
     * Cache handle to the object pool we use for the children
     */
    private FastObjectPool<E> children_pool;
    /**
     * If we reach one of these elements, we will halt the traversal
     */
    private Set<E> stop_elements;
    /**
     * Depth limit (for debugging)
     */
    private int depth_limit = -1;
    
    private boolean invoked_finished = false;

    // ----------------------------------------------------------------------
    // POOLABLE METHODS
    // ----------------------------------------------------------------------

    @Override
    public boolean isInitialized() {
        return (this.depth == -1);
    }

    @Override
    public void finish() {
        this.stack.clear();
        this.visited.clear();
        this.first = null;
        this.depth = -1;
        this.stop = false;
        this.allow_revisit = false;
        this.invoked_finished = false;
        this.counter = 0;
        this.depth_limit = -1;
        if (this.stop_elements != null)
            this.stop_elements.clear();

        if (this.children_pool != null) {
            try {
                for (Children<E> c : this.attached_children.values()) {
                    this.children_pool.returnObject(c);
                } // FOR
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }
        this.attached_children.clear();
        this.children_pool = null;
    }

    // ----------------------------------------------------------------------
    // UTILITY METHODS
    // ----------------------------------------------------------------------

    /**
     * Initialize the children object pool needed by this object
     */
    @SuppressWarnings("unchecked")
    private void initChildrenPool(E element) {
        // Grab the handle to the children object pool we'll need
        Class<?> elementClass = element.getClass();
        synchronized (CHILDREN_POOLS) {
            this.children_pool = (FastObjectPool<E>)CHILDREN_POOLS.get(elementClass);
            if (this.children_pool == null) {
                this.children_pool = new FastObjectPool<E>(new ChildrenFactory<E>());
                CHILDREN_POOLS.put(elementClass, this.children_pool);
            }
        } // SYNCH
    }

    /**
     * Return a Children singleton for a specific element This allows us to add
     * children either before our element is invoked
     * 
     * @param element
     * @return
     */
    @SuppressWarnings("unchecked")
    protected final Children<E> getChildren(E element) {
        Children<E> c = this.attached_children.get(element);
        if (c == null) {
            if (this.children_pool == null)
                this.initChildrenPool(element);
            try {
                c = (Children<E>) this.children_pool.borrowObject();
            } catch (Exception ex) {
                throw new RuntimeException("Failed to borrow object for " + element, ex);
            }
            c.init(element);
            this.attached_children.put(element, c);
        }
        return (c);
    }

    /**
     * Returns the parent of the current node in callback()
     * 
     * @return
     */
    protected final E getPrevious() {
        E ret = null;
        int size = this.stack.size();
        if (size > 1)
            ret = this.stack.get(size - 2);
        return (ret);
    }

    /**
     * Returns the depth of the current callback() invocation
     * 
     * @return
     */
    protected final int getDepth() {
        return (this.depth);
    }

    /**
     * Returns the current visitation stack for vertices
     * 
     * @return
     */
    protected final Stack<E> getStack() {
        return (this.stack);
    }

    /**
     * Returns true if the given element has already been visited
     * 
     * @param element
     * @return
     */
    protected boolean hasVisited(E element) {
        return (this.visited.contains(element));
    }

    /**
     * Mark the given element as having been already visited
     * 
     * @param element
     */
    protected void markAsVisited(E element) {
        this.visited.add(element);
    }

    /**
     * Returns the ordered list of elements that were visited
     * 
     * @return
     */
    public List<E> getVisitPath() {
        return (Collections.unmodifiableList(this.visited));
    }

    /**
     * Toggle whether the walker is allowed to revisit a vertex more than one
     * 
     * @param flag
     */
    public void setAllowRevisit(boolean flag) {
        this.allow_revisit = flag;
    }

    /**
     * Returns the first element that initiated the traversal
     * 
     * @return
     */
    protected final E getFirst() {
        return (this.first);
    }

    /**
     * Returns the count for the number of nodes that we have visited
     * 
     * @return
     */
    public final int getCounter() {
        return (this.counter);
    }

    /**
     * Set the depth limit. Once this reached we will dump out the stack
     * 
     * @param limit
     */
    protected final void setDepthLimit(int limit) {
        this.depth_limit = limit;
    }

    /**
     * The callback() method can call this if it wants the walker to break out
     * of the traversal
     */
    protected synchronized final void stop() {
        if (this.stop == false)
            this.callback_stop();
        this.stop = true;
    }

    /**
     * Returns true is this walker has been marked as stopped
     * 
     * @return
     */
    protected final boolean isStopped() {
        return (this.stop);
    }

    /**
     * Mark an element that will cause the traversal to stop when if traverse is
     * invoked with it
     * 
     * @param element
     */
    protected synchronized final void stopAtElement(E element) {
        if (this.stop_elements == null) {
            this.stop_elements = new HashSet<E>();
        }
        this.stop_elements.add(element);
    }

    // ----------------------------------------------------------------------
    // TRAVERSAL METHODS
    // ----------------------------------------------------------------------

    public final void traverse(Collection<E> elements) {
        for (E e : elements) {
            this.traverse(e);
        } // FOR
    }

    /**
     * Depth first traversal
     * @param element
     */
    public final void traverse(E element) {
        final boolean trace = LOG.isTraceEnabled();
        assert (element != null) : "AbstractTreeWalker.innerTraverse() was passed a null element";

        if (trace)
            LOG.trace("innerTraverse(" + element + ")");
        if (this.first == null && this.stop == false) {
            assert (this.counter == 0) : "Unexpected counter value on first element [" + this.counter + "]";
            this.first = element;

            // Grab the handle to the children object pool we'll need
            this.initChildrenPool(element);

            if (trace)
                LOG.trace("callback_first(" + element + ")");
            this.callback_first(element);
        }

        // Check if we should stop here
        this.stop = this.stop || (this.stop_elements != null && this.stop_elements.contains(element));
        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
            return;
        }

        this.stack.push(element);
        this.depth++;
        this.counter++;
        if (trace)
            LOG.trace("[Stack=" + this.stack.size() + ", " + "Depth=" + this.depth + ", " + "Counter=" + this.counter + ", " + "Visited=" + this.visited.size() + "]");

        // Stackoverflow check
        if (this.depth_limit >= 0 && this.depth > this.depth_limit) {
            LOG.fatal("Reached depth limit [" + this.depth + "]");
            System.err.println(StringUtil.join("\n", Thread.currentThread().getStackTrace()));
            System.exit(1);
        }

        if (trace)
            LOG.trace("callback_before(" + element + ")");
        this.callback_before(element);
        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            return;
        }

        // Get the list of children to visit before and after we call ourself
        AbstractTreeWalker.Children<E> children = this.getChildren(element);
        this.populate_children(children, element);
        if (trace)
            LOG.trace("Populate Children: " + children);

        for (E child : children.before_list) {
            if (this.allow_revisit || this.visited.contains(child) == false) {
                if (trace)
                    LOG.trace("Traversing child " + child + "' before " + element);
                this.traverse(child);
            }
            if (this.stop)
                break;
        } // FOR
        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
            return;
        }

        // Why is this here and not up above when we update the stack?
        this.visited.add(element);

        if (trace)
            LOG.trace("callback(" + element + ")");
        this.callback(element);

        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
            return;
        }

        for (E child : children.after_list) {
            if (this.stop)
                return;
            if (this.allow_revisit || this.visited.contains(child) == false) {
                if (trace)
                    LOG.trace("Traversing child " + child + " after " + element);
                this.traverse(child);
            }
            if (this.stop)
                break;
        } // FOR
        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
            return;
        }

        E check_exp = this.stack.pop();
        assert (element.equals(check_exp)) :
            String.format("%s != %s", element, check_exp);

        this.callback_after(element);
        if (this.stop) {
            if (trace)
                LOG.trace("Stop Called. Halting traversal.");
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
            return;
        }

        this.depth--;
        if (this.depth == -1) {
            // We need to return all of the children here, because most
            // instances are not going to be pooled,
            // which means that finish() will not likely be called
            try {
                for (Children<E> c : this.attached_children.values()) {
                    this.children_pool.returnObject(c);
                } // FOR
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            this.attached_children.clear();

            if (trace)
                LOG.trace("callback_last(" + element + ")");
            this.callback_last(element);
            if (this.invoked_finished == false) {
                this.callback_finish();
                this.invoked_finished = true;
            }
        }
        return;
    }

    /**
     * For the given element, populate the Children object with the next items
     * to visit either before or after the callback method is called for the
     * element
     * 
     * @param element
     */
    protected abstract void populate_children(AbstractTreeWalker.Children<E> children, E element);

    // ----------------------------------------------------------------------
    // CALLBACK METHODS
    // ----------------------------------------------------------------------

    /**
     * This method will be called after the walker has explored a node's children
     * @param element
     *            the object to perform an operation on
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
     * Optional callback method after we have finished visiting all of a node's
     * children and will return back to their parent.
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

    /**
     * Optional callback method when stop() is called
     */
    protected void callback_stop() {
        // Do nothing
    }
    
    /**
     * Optional callback method that is called at the very end, regardless
     * of whether the traversal is ending because of stop() or because it
     * reached the end of the tree
     * This won't be called 
     */
    protected void callback_finish() {
        // Do nothing
    }
}