/**
 * 
 */
package edu.brown.hashing;

import java.util.HashMap;
import java.util.Map;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.CatalogContext;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.JSONUtil;

/**
 * @author pavlo
 */
public class MappedHasher extends AbstractHasher {
    
    public enum Members {
        HASH_TO_PARTITON;
    }
    
    /**
     * Value Hash -> Partition #
     */
    public final Map<Integer, Integer> hash_to_partition = new HashMap<Integer, Integer>();

    /**
     * @param catalog_db
     * @param num_partitions
     */
    public MappedHasher(CatalogContext catalogContext, int num_partitions) {
        super(catalogContext, num_partitions);
    }
    
    @Override
    public void init(CatalogContext catalogDb) {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * Map a hash value to particular partition
     * @param hash
     * @param partition
     */
    public void map(int hash, int partition) {
        this.hash_to_partition.put(hash, partition);
    }

    @Override
    public int hash(Object value) {
        return (this.hash(value, this.num_partitions));
    }
    
    @Override
    public int hash(Object value, CatalogType catalogItem) {
        return (this.hash(value));
    }
    
    @Override
    public int hash(Object value, int num_partitions) {
        int hash = TheHashinator.hashToPartition(value, num_partitions);
        assert(this.hash_to_partition.containsKey(hash));
        return (this.hash_to_partition.get(hash));
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, MappedHasher.class, MappedHasher.Members.values());
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, MappedHasher.class, MappedHasher.Members.values());
    }    
}
