package edu.brown.gui.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogDiffEngine;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.IndexType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.statistics.Histogram;
import edu.brown.statistics.HistogramUtil;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.StringUtil;

public class CatalogSummaryUtil {
    
    final Catalog catalog;
    
    public CatalogSummaryUtil(Catalog catalog) {
        this.catalog = catalog;
    }
    
    public Map<String, Integer>[] getProceduresInfo(boolean full) {
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        Map<String, Integer> m[] = (Map<String, Integer>[])new Map<?, ?>[1];
        int idx = -1;

        int procs = 0;
        int sysprocs = 0;
        int params = 0;
        int stmts = 0;
        for (Procedure p : catalog_db.getProcedures()) {
            if (p.getSystemproc()) {
                sysprocs++;
            } else {
                procs++;
                params += p.getParameters().size();
                stmts += p.getStatements().size();
            }
        } // FOR
        
        m[++idx] = new LinkedHashMap<String, Integer>();
        m[idx].put("Procedures", procs);
        m[idx].put("Procedure Parameters", params);
        m[idx].put("Statements", stmts);
        m[idx].put("System Procedures", sysprocs);
        
        return (m);
    }
    
    public Map<String, Integer>[] getTablesInfo(boolean full) {
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        Map<String, Integer> m[] = (Map<String, Integer>[])new Map<?, ?>[3];
        int idx = -1;
        
        int cols = 0;
        int fkeys = 0;
        int tables = 0;
        
        int idx_total = 0;
        int idx_unique = 0;
        Histogram<IndexType> idx_types = new ObjectHistogram<IndexType>();

        for (Table t : CatalogUtil.getDataTables(catalog_db)) {
            tables++;
            
            // COLUMNS
            cols += t.getColumns().size();
            for (Column c : t.getColumns()) {
                Column fkey = CatalogUtil.getForeignKeyParent(c);
                if (fkey != null) fkeys++;
            }
            
            // INDEXES
            idx_total += t.getIndexes().size();
            for (Index i : t.getIndexes()) {
                if (i.getUnique()) {
                    idx_unique++;
                }
                IndexType idx_type = IndexType.get(i.getType());
                idx_types.put(idx_type);
            } // FOR
            
        } // FOR
        
        // ----------------------
        // TABLE INFO
        // ----------------------
        m[++idx] = new LinkedHashMap<String, Integer>();
        m[idx].put("Tables", tables);
        m[idx].put("Replicated Tables", CatalogUtil.getReplicatedTables(catalog_db).size());
        m[idx].put("Views", CatalogUtil.getViewTables(catalog_db).size());
        m[idx].put("Vertical Partition Replicas", CatalogUtil.getVerticallyPartitionedTables(catalog_db).size());
        m[idx].put("Evictable Tables", CatalogUtil.getEvictableTables(catalog_db).size());
        m[idx].put("System Tables", CatalogUtil.getSysTables(catalog_db).size());
        
        // ----------------------
        // INDEX INFO
        // ----------------------
        m[++idx] = new LinkedHashMap<String, Integer>();
        m[idx].put("Indexes", idx_total);
        m[idx].put("Unique Indexes", idx_unique);
        for (IndexType idx_type : idx_types.values()) {
            m[idx].put(String.format(" + %s Indexes", idx_type.name()),
                       (int)idx_types.get(idx_type, 0));
        } // FOR
        
        // ----------------------
        // COLUMN INFO
        // ----------------------
        m[++idx] = new LinkedHashMap<String, Integer>();
        m[idx].put("Columns", cols);
        m[idx].put("Foreign Keys", fkeys);
        
        return (m);
    }
    
    public Map<String, Integer>[] getHostsInfo(boolean full) {
        Cluster catalog_clus = CatalogUtil.getCluster(this.catalog);
        Map<String, Integer> m[] = (Map<String, Integer>[])new Map<?, ?>[2];
        int idx = -1;
        
        m[++idx] = new LinkedHashMap<String, Integer>();
        m[idx].put("Hosts", catalog_clus.getHosts().size());
        m[idx].put("Sites", catalog_clus.getSites().size());
        m[idx].put("Partitions", CatalogUtil.getNumberOfPartitions(catalog_clus));
        
        return (m);
    }
    
    /**
     * Return summary text about the catalog
     */
    public String getSummaryText() {
        Collection<Map<String, Integer>> maps = new ArrayList<Map<String, Integer>>();
        
        // ----------------------
        // TABLE + COLUMN INFO
        // ----------------------
        CollectionUtil.addAll(maps, this.getTablesInfo(false));

        // ----------------------
        // PROCEDURES INFO
        // ----------------------
        CollectionUtil.addAll(maps, this.getProceduresInfo(false));
        
        // ----------------------
        // HOST INFO
        // ----------------------
        CollectionUtil.addAll(maps, this.getHostsInfo(false));
        
        return (StringUtil.formatMaps(maps.toArray(new Map<?, ?>[0])));
    }
}
