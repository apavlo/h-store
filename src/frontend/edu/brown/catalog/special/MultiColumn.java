package edu.brown.catalog.special;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.utils.Pair;

import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class MultiColumn extends Column implements MultiAttributeCatalogType<Column> {
    public static final String PREFIX = "*MultiColumn*"; 
    private static final Map<Pair<Column, Column>, MultiColumn> SINGLETONS = new HashMap<Pair<Column, Column>, MultiColumn>();

    private final Pair<Column, Column> attributes;
    
    private MultiColumn(Pair<Column, Column> attributes) {
        this.attributes = attributes;
        assert(!this.attributes.getFirst().equals(this.attributes.getSecond())) : "Duplicate Attributes: " + this.attributes;
        assert(this.attributes.getFirst().getParent().equals(this.attributes.getSecond().getParent()));
    }
    
    @Override
    public Iterator<Column> iterator() {
        return new Iterator<Column>() {
            private int idx = 0;
            @Override
            public boolean hasNext() {
                return (idx < 2);
            }
            @Override
            public Column next() {
                return (Column)MultiColumn.this.attributes.get(this.idx++);
            }
            @Override
            public void remove() {
                throw new RuntimeException("Remove is not supported! Are you crazy?");
            }
        };
    }

    @Override
    public String getPrefix() {
        return PREFIX;
    }
    @Override
    public int size() {
        return (2);
    }
    @Override
    public Column get(int idx) {
        return ((Column)this.attributes.get(idx));
    }
    @Override
    public boolean contains(Column c) {
        return (this.attributes.contains(c));
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public <T extends CatalogType> T getParent() {
        return (T)this.attributes.getFirst().getParent();
    }
    @Override
    public Catalog getCatalog() {
        return this.attributes.getFirst().getCatalog();
    }
    @Override
    public String getName() {
        return (this.getTypeName());
    }
    @Override
    public String getTypeName() {
        String names[] = new String[this.size()];
        for (int i = 0; i < names.length; i++) {
            names[i] = this.get(i).getName();
        }
        return ("<" + StringUtil.join(",", names) + ">");
    }
    @Override
    public int hashCode() {
        return this.attributes.hashCode();
    }
    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof MultiColumn)) return (false);
        return (this.attributes.equals(((MultiColumn)obj).attributes));
    }
    
    public static MultiColumn get(Column...cols) {
        assert(cols.length == 2);
        int idx0 = 0;
        int idx1 = 1;
        if (cols[0].getIndex() > cols[1].getIndex()) {
            idx0 = 1;
            idx1 = 0;
        }
        
        Pair<Column, Column> p = Pair.of(cols[idx0], cols[idx1]);
        MultiColumn obj = SINGLETONS.get(p);
        if (obj == null) {
            obj = new MultiColumn(p);
            SINGLETONS.put(p, obj);
        }
        assert(obj != null) : "Invalid MultiColumn for " + p;
        return (obj);
    }
}
