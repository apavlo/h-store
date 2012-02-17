package edu.brown.catalog.special;

import java.util.HashMap;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;

/**
 * Placeholder used to indicate that no partitioning parameter for the stored
 * procedure has been selected
 * 
 * @author pavlo
 */
public class NullProcParameter extends SpecialProcParameter {
    public static final int PARAM_IDX = -1;
    public static final String PARAM_NAME = "*NULL*";
    private static final Map<Procedure, NullProcParameter> SINGLETONS = new HashMap<Procedure, NullProcParameter>();

    private final Procedure parent;

    private NullProcParameter(Procedure parent) {
        this.parent = parent;
    }

    @Override
    public String getName() {
        return (this.getTypeName());
    }

    @Override
    public String getTypeName() {
        return (PARAM_NAME);
    }

    @Override
    public int getIndex() {
        return (PARAM_IDX);
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
        if (!(obj instanceof NullProcParameter))
            return (false);

        NullProcParameter other = (NullProcParameter) obj;
        return (this.parent.equals(other.parent));
    }

    public static synchronized NullProcParameter singleton(Procedure catalog_proc) {
        assert (catalog_proc != null);
        NullProcParameter obj = SINGLETONS.get(catalog_proc);
        if (obj == null) {
            obj = new NullProcParameter(catalog_proc);
            SINGLETONS.put(catalog_proc, obj);
        }
        assert (obj != null) : "Invalid NullProcParameter for " + catalog_proc;
        return (obj);
    }
}
