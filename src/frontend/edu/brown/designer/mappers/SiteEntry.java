package edu.brown.designer.mappers;

import java.util.HashSet;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;

import edu.brown.catalog.CatalogKey;

public class SiteEntry implements JSONString, Comparable<SiteEntry> {
    private static final long serialVersionUID = 1L;

    public enum Members {
        ID, HOST, FRAGMENTS,
    }

    private int id;
    private final Set<FragmentEntry> fragments = new HashSet<FragmentEntry>();
    private String host_key;
    private long estimated_size = 0l;
    private long estimated_heat = 0l;

    /**
     * Basic Constructor
     */
    protected SiteEntry() {
        // Do nothing...
    }

    public SiteEntry(int id) {
        this.id = id;
    }

    @Override
    public int compareTo(SiteEntry o) {
        return (this.id < o.id ? -1 : (this.id == o.id ? 0 : 1));
    }

    @Override
    public int hashCode() {
        return (this.id);
    }

    public int getId() {
        return (this.id);
    }

    public Set<FragmentEntry> getFragments() {
        return (this.fragments);
    }

    public boolean add(FragmentEntry fragment) {
        if (this.fragments.add(fragment)) {
            this.estimated_size += fragment.getEstimatedSize();
            this.estimated_heat += fragment.getEstimatedHeat();
            return (true);
        }
        return (false);
    }

    public boolean remove(FragmentEntry fragment) {
        if (this.fragments.remove(fragment)) {
            this.estimated_size -= fragment.getEstimatedSize();
            assert (this.estimated_size >= 0);
            this.estimated_heat -= fragment.getEstimatedHeat();
            assert (this.estimated_heat >= 0);
            return (true);
        }
        return (false);
    }

    public void setEstimatedSize(long estimated_size) {
        this.estimated_size = estimated_size;
    }

    public long getEstimatedSize() {
        return estimated_size;
    }

    public void setEstimatedHeat(long estimated_heat) {
        this.estimated_heat = estimated_heat;
    }

    public long getEstimatedHeat() {
        return estimated_heat;
    }

    public Host getHost(Database catalog_db) {
        return (CatalogKey.getFromKey(catalog_db, this.host_key, Host.class));
    }

    public String getHostKey() {
        return (this.host_key);
    }

    public void setHost(Host catalog_host) {
        this.setHostKey(CatalogKey.createKey(catalog_host));
    }

    public void setHostKey(String host_key) {
        this.host_key = host_key;
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
        stringer.key(Members.ID.name()).value(this.id);
        stringer.key(Members.HOST.name()).value(this.host_key);

        stringer.key(Members.FRAGMENTS.name()).array();
        for (FragmentEntry fragment : this.fragments) {
            stringer.object();
            fragment.toJSONString(stringer);
            stringer.endObject();
        }
        stringer.endArray();
    }

    public void fromJSONObject(JSONObject object, Database catalog_db) throws JSONException {
        this.id = object.getInt(Members.ID.name());
        this.host_key = object.getString(Members.HOST.name());

        this.fragments.clear();
        JSONArray jsonArray = object.getJSONArray(Members.FRAGMENTS.name());
        for (int i = 0, cnt = jsonArray.length(); i < cnt; i++) {
            JSONObject jsonObject = jsonArray.getJSONObject(i);
            FragmentEntry fragment = new FragmentEntry();
            fragment.fromJSONObject(jsonObject, catalog_db);
            this.add(fragment);
        } // FOR
    }

    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "[" + this.getId() + "]");
    }
}
