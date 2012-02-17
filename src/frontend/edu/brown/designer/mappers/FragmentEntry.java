package edu.brown.designer.mappers;

import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;

public class FragmentEntry implements JSONString {

    public enum Members {
        TABLE_KEY, HASH_KEY, ESTIMATED_SIZE, ESTIMATED_HEAT,
    }

    private String table_key;
    private int hash_key;
    private long estimated_size = 0;
    private long estimated_heat = 0;

    public FragmentEntry() {
        // Do nothing...
    }

    public FragmentEntry(String table_key, int hash) {
        this.table_key = table_key;
        this.hash_key = hash;
    }

    public FragmentEntry(Table catalog_tbl, int hash) {
        this(CatalogKey.createKey(catalog_tbl), hash);
    }

    public String getTableKey() {
        return (this.table_key);
    }

    public int getHashKey() {
        return (this.hash_key);
    }

    public void setEstimatedSize(long estimated_size) {
        this.estimated_size = estimated_size;
    }

    public long getEstimatedSize() {
        return (this.estimated_size);
    }

    public void setEstimatedHeat(long estimated_heat) {
        this.estimated_heat = estimated_heat;
    }

    public long getEstimatedHeat() {
        return (this.estimated_heat);
    }

    public Table getTable(Database catalog_db) {
        return (CatalogKey.getFromKey(catalog_db, this.table_key, Table.class));
    }

    /**
     * 
     */
    @Override
    public String toJSONString() {
        JSONStringer stringer = new JSONStringer();
        try {
            stringer.object();
            this.toJSONString(stringer);
            stringer.endObject();
        } catch (JSONException e) {
            e.printStackTrace();
            System.exit(-1);
        }
        return stringer.toString();
    }

    public void toJSONString(JSONStringer stringer) throws JSONException {
        stringer.key(Members.TABLE_KEY.name()).value(this.table_key);
        stringer.key(Members.HASH_KEY.name()).value(this.hash_key);
        stringer.key(Members.ESTIMATED_SIZE.name()).value(this.estimated_size);
        stringer.key(Members.ESTIMATED_HEAT.name()).value(this.estimated_heat);
    }

    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        this.table_key = object.getString(Members.TABLE_KEY.name());
        this.hash_key = object.getInt(Members.HASH_KEY.name());
        this.estimated_size = object.getLong(Members.ESTIMATED_SIZE.name());
        this.estimated_heat = object.getLong(Members.ESTIMATED_HEAT.name());
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("[").append(CatalogKey.getNameFromKey(this.getTableKey())).append("-").append(this.getHashKey()).append("]");
        return (buffer.toString());
    }
}
