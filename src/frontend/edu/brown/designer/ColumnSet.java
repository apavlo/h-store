package edu.brown.designer;

import java.util.*;

import org.apache.log4j.Logger;

import org.voltdb.catalog.*;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class ColumnSet extends LinkedHashSet<ColumnSet.Entry> {
    private static final long serialVersionUID = -7735075759916955292L;
    private static final Logger LOG = Logger.getLogger(ColumnSet.class.getName());

    private final Set<Statement> catalog_stmts = new HashSet<Statement>();
    
    public static class Entry extends Pair<CatalogType, CatalogType> {
        protected final ExpressionType comparison_exp;
        protected final Set<QueryType> query_types = new HashSet<QueryType>();

        public Entry(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, QueryType query_type) {
            super(element0, element1);
            this.comparison_exp = comparison_exp;
            this.query_types.add(query_type);
        }
        
        public Entry(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, Collection<QueryType> query_types) {
            super(element0, element1);
            this.comparison_exp = comparison_exp;
            this.query_types.addAll(query_types);
        }

        /**
         * @return the comparison_exp
         */
        public ExpressionType getComparisonExp() {
            return this.comparison_exp;
        }
        
        /**
         * 
         * @return
         */
        public Set<QueryType> getQueryTypes() {
            return query_types;
        }
        
        /**
         * Given one of the items of this entry, return the other entry
         * @param item
         * @return
         */
        public CatalogType getOther(CatalogType item) {
            if (this.get(0).equals(item)) {
                return ((CatalogType)this.get(1));
            } else if (this.get(1).equals(item)) {
                return ((CatalogType)this.get(0));
            }
            return (null);
        }
        
        @Override
        public String toString() {
            String ret = "( ";
            ret += CatalogUtil.getDisplayName(this.getFirst()) + " ";
            ret += ExpressionUtil.EXPRESSION_STRING.get(this.getComparisonExp());
            ret += " " + CatalogUtil.getDisplayName(this.getSecond());
            ret += " )";
            return (ret);
        }
    } // END CLASS
    
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

    public Set<Statement> getStatements() {
        return this.catalog_stmts;
    }
    
    public ColumnSet.Entry get(int idx) {
        return (CollectionUtil.get(this, idx));
    }
    
    /**
     * 
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
     * 
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
     * 
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
        boolean ret = this.add(new Entry(element0, element1, comparison_exp, query_types));
        if (ret) this.catalog_stmts.addAll(catalog_stmts);
        return (ret);
    }

    /**
     * 
     * @param match_class
     * @param search_key
     * @return
     */
    public ColumnSet createColumnSetForParent(Class<? extends CatalogType> match_class, CatalogType parent_search_key) {
        ColumnSet ret = new ColumnSet(this.catalog_stmts);
        //
        // We're looking for Pairs where one of the elements matches the search_key, and
        // the other element is of the same type of match_class
        //
        for (Entry pair : this) {
            if (pair.getFirst().getClass() == match_class && pair.getSecond().getParent() == parent_search_key) {
                ret.add(new Entry(pair.getSecond(), pair.getFirst(), pair.getComparisonExp(), pair.getQueryTypes()));
            } else if (pair.getSecond().getClass() == match_class && pair.getFirst().getParent() == parent_search_key) {
                ret.add(pair);
            }
        } // FOR
        return (ret);
    }

    public <T extends CatalogType> Set<T> findAll(Class<T> match_class, CatalogType search_key) {
        return (this.findAll(match_class, search_key, false, false));
    }
    /**
     * Find all entries in the ColumnSet where the one element of the pair matches the search_key and
     * the other element matches the given class
     * @param <T>
     * @param match_class
     * @param search_key
     * @return
     */
    public <T extends CatalogType> Set<T> findAllForOther(Class<T> match_class, CatalogType search_key) {
        return (this.findAll(match_class, search_key, false, true));
    }
    public <T extends CatalogType> Set<T> findAllForParent(Class<T> match_class, CatalogType parent_search_key) {
        return (this.findAll(match_class, parent_search_key, true, false));
    }
    public <T extends CatalogType> Set<T> findAllForOtherParent(Class<T> match_class, CatalogType parent_search_key) {
        return (this.findAll(match_class, parent_search_key, true, true));
    }
    
    /**
     * 
     * @param <T>
     * @param match_class
     * @param search_key
     * @param use_parent if set to true, the search_key is applied to the search target's parent
     * @param use_other if set to true, the search target is always the opposite of the item of 
     *                  the element that we check the match_class to
     * @return
     */
    @SuppressWarnings("unchecked")
    private <T extends CatalogType> Set<T> findAll(Class<T> match_class, CatalogType search_key, boolean use_parent, boolean use_other) {
        Set<T> found = new HashSet<T>();
        final int use_my_idxs[][] = { { 0, 0 }, { 1, 1 } };
        final int use_other_idxs[][] = { { 0, 1 }, { 1, 0 } };
        int lookup_idxs[][] = (use_other ? use_other_idxs : use_my_idxs);
        
        //
        // We're looking for Pairs where one of the elements matches the search_key, and
        // the other element is of the same type of match_class
        //
        //System.out.println("findAllUsingParent(match_class=" + match_class + ", parent_search_key=" + parent_search_key + ", use_other_parent=" + use_other_parent + ")");
        for (Pair<CatalogType, CatalogType> pair : this) {
            //System.out.println(pair);
            //int ctr = 0;
            for (int idxs[] : lookup_idxs) {
                T cur = (T)pair.get(idxs[0]);
                Class<?> cur_class = pair.get(idxs[0]).getClass();
                CatalogType cur_value = (CatalogType)pair.get(idxs[1]);
                if (use_parent) cur_value = cur_value.getParent();
                
                //System.out.println("[" + ctr + "] cur: " + cur); 
                //System.out.println("[" + ctr + "] class: " + cur_class.equals(match_class) + " " + cur_class);
                //System.out.println("[" + ctr++ + "] parent: " + cur_parent.equals(parent_search_key) + " " + cur_parent);
                
                if (cur_class.equals(match_class) && cur_value.equals(search_key)) {
                    found.add(cur);
                    break;
                }
            } // FOR
        } // FOR
        return (found);
    }
    
    /**
     * 
     * @param search_key
     * @return
     */
    public Set<Entry> findAll(CatalogType search_key) {
        Set<Entry> found = new HashSet<Entry>();
        for (Entry entry : this) {
            if (entry.contains(search_key)) {
                found.add(entry);
            }
        } // FOR
        return (found);
    }
    
    /**
     * 
     * @param search_key
     * @return
     */
    public Set<Entry> findAllForParent(CatalogType search_key) {
        Set<Entry> found = new HashSet<Entry>();
        for (Entry entry : this) {
            if (entry.getFirst().getParent().equals(search_key) ||
                entry.getSecond().getParent().equals(search_key)) {
                found.add(entry);
            }
        } // FOR
        return (found);
    }
    
    public <T extends CatalogType> Set<T> findAllForType(Class<T> search_key) {
        Set<T> found = new HashSet<T>();
        for (Entry e : this) {
            if (ClassUtil.getSuperClasses(e.getFirst().getClass()).contains(search_key)) {
                found.add((T)e.getFirst());
            }
            if (ClassUtil.getSuperClasses(e.getSecond().getClass()).contains(search_key)) {
                found.add((T)e.getSecond());
            }
        } // FOR
        return (found);
    }
    
    public <T extends CatalogType> Histogram buildHistogramForType(Class<T> search_key) {
        Histogram h = new Histogram();
        for (Entry e : this) {
            if (ClassUtil.getSuperClasses(e.getFirst().getClass()).contains(search_key)) {
                h.put((T)e.getFirst());
            }
            if (ClassUtil.getSuperClasses(e.getSecond().getClass()).contains(search_key)) {
                h.put((T)e.getSecond());
            }
        } // FOR
        return (h);
    }
    
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return (true);
        if (!(o instanceof ColumnSet)) return (false);
        
        //
        // Otherwise, we need to loop through each of our Pairs and see if there
        // is a matching Pair of items on the other side
        //
        ColumnSet cset = (ColumnSet)o;
        return (this.containsAll(cset) && cset.containsAll(this));
    }

    public String debug() {
        String ret = "ColumnSet: {\n";
        for (Entry pair : this) {
            ret += StringUtil.SPACER + pair.toString() + "\n";
        } // FOR
        ret += "}";
        return (ret);
    }
    
}
