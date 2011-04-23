package edu.brown.hashing;

import org.apache.commons.collections15.map.LRUMap;
import org.voltdb.catalog.Database;

public class CachedHasher extends DefaultHasher {

    private final LRUMap<Object, Integer> cache = new LRUMap<Object, Integer>(2048);

    /**
     * Constructor
     * @param catalog_db
     * @param num_partitions
     */
    public CachedHasher(Database catalog_db, Integer num_partitions) {
        super(catalog_db, num_partitions);
    }

    @Override
    public void init(Database catalogDb) {
        super.init(catalogDb);
        this.cache.clear();
    }
    
    @Override
    public int hash(Object value, int num_partitions) {
        Integer hash = this.cache.get(value);
        if (hash == null) {
            hash = super.hash(value, num_partitions);
            this.cache.put(value, hash);
        }
        return (hash.intValue());
    }
}
