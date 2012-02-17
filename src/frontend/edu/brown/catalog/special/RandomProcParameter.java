package edu.brown.catalog.special;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Procedure;

/**
 * Placeholder used to indicate that no partitioning parameter for the stored
 * procedure has been selected
 * 
 * @author pavlo
 */
public class RandomProcParameter extends SpecialProcParameter {
    public static final int PARAM_IDX = -2;
    public static final String PARAM_NAME = "*RANDOM*";
    private static final Map<Procedure, RandomProcParameter> SINGLETONS = new HashMap<Procedure, RandomProcParameter>();

    // HACK
    public static final Random rand = new Random();

    private final Procedure parent;

    private RandomProcParameter(Procedure parent) {
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
        if (!(obj instanceof RandomProcParameter))
            return (false);

        RandomProcParameter other = (RandomProcParameter) obj;
        return (this.parent.equals(other.parent));
    }

    public static synchronized RandomProcParameter singleton(Procedure catalog_proc) {
        assert (catalog_proc != null);
        RandomProcParameter obj = SINGLETONS.get(catalog_proc);
        if (obj == null) {
            obj = new RandomProcParameter(catalog_proc);
            SINGLETONS.put(catalog_proc, obj);
        }
        assert (obj != null) : "Invalid RandomProcParameter for " + catalog_proc;
        return (obj);
    }
}
