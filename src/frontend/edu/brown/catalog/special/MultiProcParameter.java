package edu.brown.catalog.special;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogUtil;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public class MultiProcParameter extends ProcParameter implements MultiAttributeCatalogType<ProcParameter> {
    public static final String PREFIX = "*MultiProcParameter*"; 
    private static final Map<Database, Map<Pair<ProcParameter, ProcParameter>, MultiProcParameter>> SINGLETONS = new HashMap<Database, Map<Pair<ProcParameter, ProcParameter>, MultiProcParameter>>();

    private final Pair<ProcParameter, ProcParameter> attributes;
    
    private MultiProcParameter(Pair<ProcParameter, ProcParameter> attributes) {
        this.attributes = attributes;
        assert(!this.attributes.getFirst().equals(this.attributes.getSecond())) : "Duplicate Attributes: " + this.attributes;
        assert(this.attributes.getFirst().getParent().equals(this.attributes.getSecond().getParent()));
    }
    
    @Override
    public Iterator<ProcParameter> iterator() {
        return new Iterator<ProcParameter>() {
            private int idx = 0;
            @Override
            public boolean hasNext() {
                return (idx < 2);
            }
            @Override
            public ProcParameter next() {
                return (ProcParameter)MultiProcParameter.this.attributes.get(this.idx++);
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
    public ProcParameter get(int idx) {
        return ((ProcParameter)this.attributes.get(idx));
    }
    @Override
    public boolean contains(ProcParameter c) {
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
        if (!(obj instanceof MultiProcParameter)) return (false);
        return (this.attributes.equals(((MultiProcParameter)obj).attributes));
    }
    
    public static MultiProcParameter get(ProcParameter...params) {
        assert(params.length == 2);
        int idx0 = 0;
        int idx1 = 1;
//        if (params[0].getIndex() > params[1].getIndex()) {
//            idx0 = 1;
//            idx1 = 0;
//        }
        Pair<ProcParameter, ProcParameter> p = Pair.of(params[idx0], params[idx1]);
        Database catalog_db = CatalogUtil.getDatabase(params[idx0]);
        if (!SINGLETONS.containsKey(catalog_db)) {
            SINGLETONS.put(catalog_db, new HashMap<Pair<ProcParameter,ProcParameter>, MultiProcParameter>());
        }
        MultiProcParameter obj = SINGLETONS.get(catalog_db).get(p);
        if (obj == null) {
            obj = new MultiProcParameter(p);
            SINGLETONS.get(catalog_db).put(p, obj);
            
            // Add the parameter object to the procedure's list
            Procedure catalog_proc = obj.getParent();
            obj.setIndex(catalog_proc.getParameters().size());
            catalog_proc.getParameters().addObject(obj);
        }
        assert(obj != null) : "Invalid MultiProcParameter for " + p;
        return (obj);
    }
}