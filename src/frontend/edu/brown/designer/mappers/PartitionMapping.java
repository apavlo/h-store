package edu.brown.designer.mappers;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import org.json.JSONException;
import org.json.JSONString;
import org.json.JSONStringer;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogKey;
import edu.brown.designer.MemoryEstimator;
import edu.brown.hashing.AbstractHasher;
import edu.brown.statistics.WorkloadStatistics;

public class PartitionMapping implements JSONString {
    private static final String SPACER = "   ";

    private final Set<FragmentEntry> fragments = new HashSet<FragmentEntry>();
    private transient final Map<FragmentEntry, SiteEntry> fragment_site_xref = new HashMap<FragmentEntry, SiteEntry>();
    private transient final Map<String, Set<FragmentEntry>> table_fragment_xref = new HashMap<String, Set<FragmentEntry>>();

    private final SortedSet<SiteEntry> sites = new TreeSet<SiteEntry>();
    private transient final Map<String, SortedSet<SiteEntry>> host_site_xref = new HashMap<String, SortedSet<SiteEntry>>();
    private transient final Map<Integer, SiteEntry> site_id_xref = new HashMap<Integer, SiteEntry>();

    /**
     * Constructor
     * 
     * @param catalogContext
     * @param pplan
     */
    public PartitionMapping() {
        // Do nothing...
    }

    public void initialize() {
        for (SiteEntry site : this.sites) {
            String host_key = site.getHostKey();
            if (!this.host_site_xref.containsKey(host_key)) {
                this.host_site_xref.put(host_key, new TreeSet<SiteEntry>());
            }
            this.host_site_xref.get(host_key).add(site);

            for (FragmentEntry fragment : site.getFragments()) {
                this.fragments.add(fragment);
                this.fragment_site_xref.put(fragment, site);

                String table_key = fragment.getTableKey();
                if (!this.table_fragment_xref.containsKey(table_key)) {
                    this.table_fragment_xref.put(table_key, new HashSet<FragmentEntry>());
                }
                this.table_fragment_xref.get(table_key).add(fragment);
            } // FOR
        } // FOR
    }

    public void apply(Database catalog_db, WorkloadStatistics stats, AbstractHasher hasher) {
        //
        // We need to estimate how big each partition is
        //
        MemoryEstimator estimator = new MemoryEstimator(stats, hasher);
        for (SiteEntry site : this.sites) {
            long site_size = 0l;
            for (FragmentEntry fragment : site.getFragments()) {
                Table catalog_tbl = fragment.getTable(catalog_db);
                Column partition_col = catalog_tbl.getPartitioncolumn();
                long size = estimator.estimate(catalog_tbl, partition_col, fragment.getHashKey());
                site_size += size;
                fragment.setEstimatedSize(size);
            } // FOR
            site.setEstimatedSize(site_size);
        } // FOR
    }

    public long getTotalSize(Host catalog_host) {
        long total = 0;
        for (SiteEntry site : this.host_site_xref.get(catalog_host)) {
            total += site.getEstimatedSize();
        } // FOR
        return (total);
    }

    /**
     * Assign the SiteEntry to a particular Host
     * 
     * @param catalog_host
     * @param site
     */
    public void assign(Host catalog_host, SiteEntry site) {
        assert (catalog_host != null);
        assert (site != null);
        if (site.getHostKey() != null) {
            this.host_site_xref.get(site.getHostKey()).remove(site);
        }
        String host_key = CatalogKey.createKey(catalog_host);
        site.setHostKey(host_key);
        if (!this.host_site_xref.containsKey(host_key)) {
            this.host_site_xref.put(host_key, new TreeSet<SiteEntry>());
        }
        this.host_site_xref.get(host_key).add(site);
        this.site_id_xref.put(site.getId(), site);
        this.sites.add(site);
    }

    /**
     * Assign the FragmentEntry to a particular SiteEntry
     * 
     * @param site
     * @param fragment
     */
    public void assign(SiteEntry site, FragmentEntry fragment) {
        assert (site != null);
        assert (fragment != null);
        site.add(fragment);
        this.fragment_site_xref.put(fragment, site);
        this.fragments.add(fragment);
    }

    /**
     * Merge the source SiteEntry into the target SiteEntry
     * 
     * @param source
     * @param target
     */
    public void merge(SiteEntry source, SiteEntry target) {
        // Copy over all the fragments
        for (FragmentEntry fragment : source.getFragments()) {
            this.assign(target, fragment);
        } // FOR

        // Remove all references to the source SiteEntry
        this.site_id_xref.remove(source.getId());
        this.host_site_xref.get(source.getHostKey()).remove(source);
    }

    public FragmentEntry getFragment(Table catalog_tbl, int hash) {
        String table_key = CatalogKey.createKey(catalog_tbl);
        for (FragmentEntry fragment : this.fragments) {
            if (fragment.getTableKey().equals(table_key) && fragment.getHashKey() == hash) {
                return (fragment);
            }
        } // FOR
        return (null);
    }

    public SiteEntry getSite(Table catalog_tbl, int hash) {
        FragmentEntry fragment = this.getFragment(catalog_tbl, hash);
        return (this.fragment_site_xref.get(fragment));
    }

    public SiteEntry getSite(int id) {
        return (this.site_id_xref.get(id));
    }

    public Set<String> getHosts() {
        return (this.host_site_xref.keySet());
    }

    public Set<Host> getHosts(Database catalog_db) {
        Set<Host> hosts = new HashSet<Host>();
        for (String host_key : this.getHosts()) {
            Host catalog_host = CatalogKey.getFromKey(catalog_db, host_key, Host.class);
            assert (catalog_host != null);
            hosts.add(catalog_host);
        } // FOR
        return (hosts);
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
        // TODO
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("Hosts:     ").append(this.getHosts().size()).append("\n");
        buffer.append("Sites:     ").append(this.sites.size()).append("\n");
        buffer.append("Fragments: ").append(this.fragments.size()).append("\n");

        SortedSet<String> hosts = new TreeSet<String>();
        hosts.addAll(this.getHosts());

        for (String host_key : hosts) {
            buffer.append("\nHost ").append(CatalogKey.getNameFromKey(host_key)).append("\n");
            for (SiteEntry site : this.host_site_xref.get(host_key)) {
                buffer.append(SPACER).append("Site ").append(site.getId()).append("\n");
                for (FragmentEntry fragment : site.getFragments()) {
                    buffer.append(SPACER).append(SPACER).append("Fragment ").append(fragment).append(" Size=").append(fragment.getEstimatedSize()).append(" Heat=").append(fragment.getEstimatedHeat())
                            .append("\n");
                } // FOR
            } // FOR
            buffer.append("--------------------");
        } // FOR
        return (buffer.toString());
    }
}
