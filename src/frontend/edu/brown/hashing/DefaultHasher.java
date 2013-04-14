/**
 * 
 */
package edu.brown.hashing;

import java.util.ArrayList;
import java.util.Arrays;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.CatalogContext;
import org.voltdb.TheHashinator;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Database;

import edu.brown.utils.ClassUtil;

/**
 * @author pavlo
 *
 */
public class DefaultHasher extends AbstractHasher {

    /**
     * @param catalogContext
     * @param num_partitions
     */
    public DefaultHasher(CatalogContext catalogContext, int num_partitions) {
        super(catalogContext, num_partitions);
    }

    public DefaultHasher(CatalogContext catalogContext) {
        this(catalogContext, catalogContext.numberOfPartitions);
    }
    
    @Override
    public void init(CatalogContext catalogDb) {
        // TODO Auto-generated method stub
        
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
    public int hash(Object value, int num_partitions) {
        assert(!ClassUtil.isArray(value)) : "Value for hashing is an array: " + Arrays.toString((Object[])value); 
        return TheHashinator.hashToPartition(value, num_partitions);
    }
    
    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        // Nothing to do
    }
    
    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        // Nothing to do
    }
    
    public static void main(String[] args) throws Exception {
        assert(args.length == 2);
        
        Integer partitions = Integer.parseInt(args[0]);
        ArrayList<Integer> values = new ArrayList<Integer>();
        for (String s : args[1].split(",")) {
            values.add(Integer.parseInt(s));
        }
        assert(!values.isEmpty());
     
        DefaultHasher hasher = new DefaultHasher(null, partitions);
        if (values.size() == 1) {
            int hash = hasher.hash(values.get(0));
            System.err.println(String.format("hash(%s, %s) = %d", values.get(0).toString(), partitions.toString(), hash));
        } else {
            int hashes[] = new int[values.size()];
            for (int i = 0; i < hashes.length; i++) {
                hashes[i] = hasher.hash(values.get(i));
                System.err.println(String.format("[%d] hash(%s, %s) = %d", i, values.get(i).toString(), partitions.toString(), hashes[i]));
            } // FOR
            int hash = hasher.multiValueHash(hashes);
            System.err.println("COMBINED: " + Arrays.toString(hashes) + " => " + hash);
        }
    }
}
