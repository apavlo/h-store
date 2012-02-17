package edu.brown.catalog.special;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

/**
 * Placeholder catalog object used to indicate that a table should be replicated
 * in our search traversal
 * 
 * @author pavlo
 */
public class ReplicatedColumn extends Column {
    public static final String COLUMN_NAME = "*REPLICATED*";
    private static final Map<Table, ReplicatedColumn> SINGLETONS = new HashMap<Table, ReplicatedColumn>();

    private final Table parent;

    private ReplicatedColumn(Table parent) {
        this.parent = parent;
    }

    @Override
    public String getName() {
        return (this.getTypeName());
    }

    @Override
    public String getTypeName() {
        return (COLUMN_NAME);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends CatalogType> T getParent() {
        return ((T) this.parent);
    }

    @Override
    public Catalog getCatalog() {
        return this.parent.getCatalog();
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof ReplicatedColumn))
            return (false);
        ReplicatedColumn other = (ReplicatedColumn) obj;
        return (this.parent.equals(other.parent));
    }

    public static synchronized ReplicatedColumn get(Table catalog_tbl) {
        assert (catalog_tbl != null);
        ReplicatedColumn obj = SINGLETONS.get(catalog_tbl);
        if (obj == null) {
            obj = new ReplicatedColumn(catalog_tbl);
            SINGLETONS.put(catalog_tbl, obj);
        }
        assert (obj != null) : "Invalid ReplicateColumn for " + catalog_tbl;
        return (obj);
    }
}
