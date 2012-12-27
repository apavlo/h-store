package edu.brown.designer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Statement;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogPair;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.StringUtil;

public class ColumnSet extends ListOrderedSet<CatalogPair> {
    private static final long serialVersionUID = -7735075759916955292L;
    private static final Logger LOG = Logger.getLogger(ColumnSet.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();

    private final Set<Statement> catalog_stmts = new HashSet<Statement>();

    /**
     * 
     */
    public ColumnSet() {
        super();
    }

    public ColumnSet(Collection<Statement> catalog_stmts) {
        this();
        this.catalog_stmts.addAll(catalog_stmts);
    }

    @Override
    public void clear() {
        super.clear();
        this.catalog_stmts.clear();
    }

    public Collection<Statement> getStatements() {
        return this.catalog_stmts;
    }

    /**
     * @return
     */
    public Map<QueryType, Integer> getQueryCounts() {
        Map<QueryType, Integer> query_counts = new HashMap<QueryType, Integer>();
        for (QueryType type : QueryType.values()) {
            query_counts.put(type, 0);
        } // FOR
        for (Statement catalog_stmt : this.catalog_stmts) {
            QueryType type = QueryType.get(catalog_stmt.getQuerytype());
            query_counts.put(type, query_counts.get(type) + 1);
        } // FOR
        return (query_counts);
    }

    public boolean add(CatalogType element0, CatalogType element1) {
        return (this.add(element0, element1, ExpressionType.COMPARE_EQUAL, new HashSet<Statement>()));
    }

    /**
     * @param element0
     * @param element1
     * @param comparison_exp
     * @param catalog_stmts
     * @return
     */
    public boolean add(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, Statement... catalog_stmts) {
        Set<Statement> stmts = new HashSet<Statement>();
        for (Statement stmt : catalog_stmts) {
            stmts.add(stmt);
        }
        return (this.add(element0, element1, comparison_exp, stmts));
    }

    /**
     * @param element0
     * @param element1
     * @param comparison_exp
     * @param catalog_stmts
     * @return
     */
    public boolean add(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, Collection<Statement> catalog_stmts) {
        Set<QueryType> query_types = new HashSet<QueryType>();
        for (Statement catalog_stmt : catalog_stmts) {
            query_types.add(QueryType.get(catalog_stmt.getQuerytype()));
        } // FOR
        boolean ret = this.add(CatalogPair.factory(element0, element1, comparison_exp, query_types));
        if (ret)
            this.catalog_stmts.addAll(catalog_stmts);
        return (ret);
    }

    /**
     * @param match_class
     * @param search_key
     * @return
     */
    public ColumnSet createColumnSetForParent(Class<? extends CatalogType> match_class, CatalogType parent_search_key) {
        ColumnSet ret = new ColumnSet(this.catalog_stmts);
        // We're looking for Pairs where one of the elements matches the search_key,
        // and the other element is of the same type of match_class
        for (CatalogPair e : this) {
            if (e.getFirst().getClass().equals(match_class) && e.getSecond().getParent().equals(parent_search_key)) {
                ret.add(CatalogPair.factory(e.getSecond(), e.getFirst(), e.getComparisonExp(), e.getQueryTypes()));
            } else if (e.getSecond().getClass().equals(match_class) && e.getFirst().getParent().equals(parent_search_key)) {
                ret.add(e);
            }
        } // FOR
        return (ret);
    }

    /**
     * Find all elements in the ColumnSet that match both the search key and the
     * given class
     * 
     * @param <T>
     * @param match_class
     * @param search_key
     * @return
     */
    public <T extends CatalogType> Collection<T> findAll(Class<T> match_class, CatalogType search_key) {
        return (this.find(match_class, search_key, false, false));
    }

    /**
     * Find all elements of the given match class in the ColumnSet where the
     * other element in the CatalogPair matches the search_key
     * 
     * @param <T>
     * @param match_class
     * @param search_key
     * @return
     */
    public <T extends CatalogType> Collection<T> findAllForOther(Class<T> match_class, CatalogType search_key) {
        return (this.find(match_class, search_key, false, true));
    }

    /**
     * Find all elements of the given match class in the ColumnSet where that
     * element's parent matches the given search key
     * 
     * @param <T>
     * @param match_class
     * @param parent_search_key
     * @return
     */
    public <T extends CatalogType> Collection<T> findAllForParent(Class<T> match_class, CatalogType parent_search_key) {
        return (this.find(match_class, parent_search_key, true, false));
    }

    /**
     * Find all elements of the given match class in the ColumnSet where the
     * other element in the CatalogPair has a parent that matches the search key
     * 
     * @param <T>
     * @param match_class
     * @param parent_search_key
     * @return
     */
    public <T extends CatalogType> Collection<T> findAllForOtherParent(Class<T> match_class, CatalogType parent_search_key) {
        return (this.find(match_class, parent_search_key, true, true));
    }

    /**
     * @param <T>
     * @param match_class
     * @param search_key
     * @param use_parent
     *            if set to true, the search_key is applied to the search
     *            target's parent
     * @param use_other
     *            if set to true, the search target is always the opposite of
     *            the item of the element that we check the match_class to
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T extends CatalogType> Collection<T> find(Class<T> match_class, CatalogType search_key, boolean use_parent, boolean use_other) {
        if (d)
            LOG.debug(String.format("find(match_class=%s, search_key=%s, use_parent=%s, use_other=%s)", match_class.getSimpleName(), search_key.fullName(), use_parent, use_other));
        assert (search_key != null) : "Invalid search key";

        Set<T> found = new HashSet<T>();
        final int use_my_idxs[][] = { { 0, 0 }, { 1, 1 } };
        final int use_other_idxs[][] = { { 0, 1 }, { 1, 0 } };
        int lookup_idxs[][] = (use_other ? use_other_idxs : use_my_idxs);

        // We're looking for Pairs where one of the elements matches the
        // search_key, and
        // the other element is of the same type of match_class
        for (Pair<CatalogType, CatalogType> pair : this) {
            if (t)
                LOG.trace(pair);
            int ctr = 0;
            for (int idxs[] : lookup_idxs) {
                T cur = (T) pair.get(idxs[0]);
                Class<?> cur_class = pair.get(idxs[0]).getClass();
                List<Class<?>> all_classes = ClassUtil.getSuperClasses(cur_class);
                CatalogType cur_value = (CatalogType) pair.get(idxs[1]);
                if (use_parent)
                    cur_value = cur_value.getParent();

                if (t) {
                    LOG.trace("[" + ctr + "] cur: " + cur);
                    LOG.trace("[" + ctr + "] class: " + cur_class.equals(match_class) + " " + cur_class);
                    LOG.trace("[" + (ctr++) + "] cur_value: " + cur_value);
                }
                if (cur_value != null && cur_value.equals(search_key) && all_classes.contains(match_class)) {
                    found.add(cur);
                    break;
                }
            } // FOR
        } // FOR
        return (found);
    }

    /**
     * @param search_keys
     * @return
     */
    public Collection<CatalogPair> findAll(CatalogType...search_keys) {
        List<CatalogPair> found = new ArrayList<CatalogPair>();
        for (CatalogPair entry : this) {
            for (CatalogType key : search_keys) {
                if (entry.contains(key)) {
                    found.add(entry);
                }
            } // FOR
        } // FOR
        return (found);
    }

    /**
     * @param search_key
     * @return
     */
    public Collection<CatalogPair> findAllForParent(CatalogType search_key) {
        List<CatalogPair> found = new ArrayList<CatalogPair>();
        for (CatalogPair entry : this) {
            if (entry.getFirst().getParent().equals(search_key) ||
                entry.getSecond().getParent().equals(search_key)) {
                found.add(entry);
            }
        } // FOR
        return (found);
    }

    @SuppressWarnings("unchecked")
    public <T extends CatalogType> Collection<T> findAllForType(Class<T> search_key) {
        List<T> found = new ArrayList<T>();
        for (CatalogPair e : this) {
            if (ClassUtil.getSuperClasses(e.getFirst().getClass()).contains(search_key)) {
                found.add((T) e.getFirst());
            }
            if (ClassUtil.getSuperClasses(e.getSecond().getClass()).contains(search_key)) {
                found.add((T) e.getSecond());
            }
        } // FOR
        return (found);
    }

    @SuppressWarnings("unchecked")
    public <T extends CatalogType> ObjectHistogram<T> buildHistogramForType(Class<T> search_key) {
        ObjectHistogram<T> h = new ObjectHistogram<T>();
        for (CatalogPair e : this) {
            if (ClassUtil.getSuperClasses(e.getFirst().getClass()).contains(search_key)) {
                h.put((T) e.getFirst());
            }
            if (ClassUtil.getSuperClasses(e.getSecond().getClass()).contains(search_key)) {
                h.put((T) e.getSecond());
            }
        } // FOR
        return (h);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return (true);
        if (!(o instanceof ColumnSet))
            return (false);

        // Otherwise, we need to loop through each of our Pairs and see if there
        // is a matching Pair of items on the other side
        ColumnSet cset = (ColumnSet) o;
        return (this.containsAll(cset) && cset.containsAll(this));
    }

    public String debug() {
        String ret = "ColumnSet: {\n";
        for (CatalogPair pair : this) {
            ret += StringUtil.SPACER + pair.toString() + "\n";
        } // FOR
        ret += "}";
        return (ret);
    }

}
