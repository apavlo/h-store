package edu.brown.hashing;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

public abstract class AbstractHasher implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(AbstractHasher.class.getName());

    protected final int num_partitions;
    protected final CatalogContext catalogContext;
    
    public AbstractHasher(CatalogContext catalogContext, int num_partitions) {
        this.catalogContext = catalogContext;
        this.num_partitions = num_partitions;
    }
    
    /**
     * Combine multiple values into a single key and get the hash of that
     * Should be uniformly distributed (or at least good enough for what we need)
     * @param values
     * @return
     */
    public int multiValueHash(Object values[]) {
        assert(values.length > 0);
        int combined = 31 * Arrays.deepHashCode(values);
        return (this.hash(combined));
    }
    public int multiValueHash(Object val0, Object val1) {
        return (this.multiValueHash(new Object[]{ val0, val1 }));
    }
    public int multiValueHash(int...values) {
        Object o[] = new Object[values.length];
        for (int i = 0; i < o.length; i++) {
            o[i] = values[i];
        }
        return this.multiValueHash(o);
    }
    
    /**
     * Return the number of partitions that this hasher can map values to
     * @return
     */
    public final int getNumPartitions() {
        return (this.num_partitions);
    }
    
    // -----------------------------------------------------------------
    // ABSTRACT INTERFACE
    // -----------------------------------------------------------------
    
    public abstract void init(CatalogContext catalogContext);
    
    /**
     * Hash the given value based on the partition count 
     * @param value
     * @return
     */
    public abstract int hash(Object value);
    
    /**
     * Hash the given value that is derived from a particular catalog object
     * @param value
     * @param catalog_item
     * @return
     */
    public abstract int hash(Object value, CatalogType catalog_item);
    
    /**
     * Hash the given value using a specific partition count
     * @param value
     * @param num_partitions
     * @return
     */
    public abstract int hash(Object value, int num_partitions);
    
    // -----------------------------------------------------------------
    // SERIALIZATION
    // -----------------------------------------------------------------
    
    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }
    
    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }
    
    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }
}
