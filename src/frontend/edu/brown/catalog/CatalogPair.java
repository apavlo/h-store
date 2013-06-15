package edu.brown.catalog;

import java.util.Collection;

import org.voltdb.catalog.CatalogType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Pair;

import edu.brown.expressions.ExpressionUtil;

public class CatalogPair extends Pair<CatalogType, CatalogType> {
    private final ExpressionType comparison_exp;
    private final QueryType query_types[];

    public static CatalogPair factory(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, Collection<QueryType> query_types) {
        return (CatalogPair.factory(element0, element1, comparison_exp, query_types.toArray(new QueryType[query_types.size()])));
    }

    public static CatalogPair factory(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, QueryType... query_types) {
        // Sort them!
        if (element0.compareTo(element1) > 0) {
            CatalogType temp = element0;
            element0 = element1;
            element1 = temp;
        }
        return (new CatalogPair(element0, element1, comparison_exp, query_types));
    }

    private CatalogPair(CatalogType element0, CatalogType element1, ExpressionType comparison_exp, QueryType query_types[]) {
        super(element0, element1);
        this.comparison_exp = comparison_exp;
        this.query_types = query_types;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof CatalogPair) {
            CatalogPair other = (CatalogPair) o;
            return (this.comparison_exp == other.comparison_exp && super.equals(other));
        }
        return (false);
    }

    /**
     * @return the comparison_exp
     */
    public ExpressionType getComparisonExp() {
        return this.comparison_exp;
    }

    /**
     * @return
     */
    public QueryType[] getQueryTypes() {
        return this.query_types;
    }
    
    public int getQueryTypeCount() {
        return (this.query_types.length);
    }
    
    public boolean containsQueryType(QueryType search) {
        for (QueryType qtype : this.query_types) {
            if (qtype == search) return (true);
        }
        return (false);
    }
    

    /**
     * Given one of the items of this entry, return the other entry
     * 
     * @param item
     * @return
     */
    public CatalogType getOther(CatalogType item) {
        if (this.get(0).equals(item)) {
            return ((CatalogType) this.get(1));
        } else if (this.get(1).equals(item)) {
            return ((CatalogType) this.get(0));
        }
        return (null);
    }

    @Override
    public String toString() {
        return (String.format("(%s %s %s)",
                this.getFirst().fullName(),
                ExpressionUtil.EXPRESSION_STRING.get(this.getComparisonExp()),
                this.getSecond().fullName()));
    }
} // END CLASS