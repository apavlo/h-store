package edu.brown.hashing;

import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.ClassUtil;

public class ConsistentHasher extends AbstractHasher {

    private long pointer;
    
    public ConsistentHasher(Database catalog_db, int num_partitions) {
        super(catalog_db, num_partitions);
    }

    @Override
    public void init(Database catalog_db) {
        // Allocate a new instance of the C++ consistent hasher
        this.pointer = this.nativeCreate();
    }

    @Override
    public int hash(Object value) {
        return (this.hash(value, this.num_partitions));
    }

    @Override
    public int hash(Object value, CatalogType catalogItem) {
        assert(!ClassUtil.isArray(value)) : "Value for hashing is an array: " + Arrays.toString((Object[])value);
        assert(catalogItem != null) : "Null catalog item [value=" + value + "]";
        int hash = this.hash(value, this.num_partitions);
        assert(hash >= 0) : "Invalid Hash [value=" + value + ", catalog=" + catalogItem + "]";
        return (hash);
    }

    @Override
    public int hash(Object obj, int num_partitions) {
        assert(!ClassUtil.isArray(obj)) : "Value for hashing is an array: " + Arrays.toString((Object[])obj); 
        
        // First get a hash for the java Object, because this is what
        // the C++ library can handle
        int value = TheHashinator.hashToPartition(obj, Integer.MAX_VALUE);
        
        // Then invoke the C++ library to figure out what partition we need to go to
        int partition = this.nativeHashinate(this.pointer, value);
        return (partition);
    }

    /**
     * Create a new invocation of our C++ ConsistentHasher
     * @return the created pointer casted to a jlong
     */
    protected native long nativeCreate();
    
    /**
     * Releases all resources held by this hasher instance
     * @param pointer the C++ ConsistentHasher pointer to be destroyed
     * @return error code
     */
    protected native int nativeDestroy(long pointer);
    
    /**
     * 
     * @param value
     * @return
     */
    public native int nativeHashinate(long pointer, int value);
    
    // IGNORE FOR NOW
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // TODO Auto-generated method stub
    }
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // TODO Auto-generated method stub
    }
    
}
