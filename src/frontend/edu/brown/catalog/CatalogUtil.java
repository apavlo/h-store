package edu.brown.catalog;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

import org.json.*;

import org.voltdb.*;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.*;
import org.voltdb.types.*;
import org.voltdb.utils.*;
import org.voltdb.catalog.*;
import org.voltdb.expressions.*;

import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

/**
 * @author pavlo
 */
public abstract class CatalogUtil extends org.voltdb.utils.CatalogUtil {
    private static final Logger LOG = Logger.getLogger(CatalogUtil.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static final String DEFAULT_CLUSTER_NAME = "cluster";
    public static final String DEFAULT_DATABASE_NAME = "database";
    public static final String DEFAULT_PROCEDURE_NAME = "procedure";
    public static final String DEFAULT_STATEMENT_NAME = "statement";

    private static final Random rand = new Random();

    // ------------------------------------------------------------
    // CACHES
    // ------------------------------------------------------------

    public static class Cache {
        
        /**
         * The set of read-only columns excluding inserts
         */
        public final Map<Table, Set<Column>> READONLY_COLUMNS_NO_INSERTS = new HashMap<Table, Set<Column>>();
        /**
         * The set of read-only columns including inserts
         */
        public final Map<Table, Set<Column>> READONLY_COLUMNS_ALL = new HashMap<Table, Set<Column>>();
        /**
         * The set of Columns referenced for each Statement
         * Statement -> Set<Column>
         */
        public final Map<Statement, Set<Column>> STATEMENT_ALL_COLUMNS = new HashMap<Statement, Set<Column>>();
        /**
         * The set of Columns referenced modified in each Statement
         * Statement -> Set<Column>
         */
        public final Map<Statement, Set<Column>> STATEMENT_MODIFIED_COLUMNS = new HashMap<Statement, Set<Column>>();
        /**
         * Statement -> Set<Table>
         */
        private final Map<Statement, Set<Table>> STATEMENT_TABLES = new HashMap<Statement, Set<Table>>();
        /**
         * Procedure -> Set<Column>
         */
        private final Map<Procedure, Set<Column>> PROCEDURE_COLUMNS = new HashMap<Procedure, Set<Column>>();
        /**
         * Procedure -> Set<Table>
         */
        private final Map<Procedure, Set<Table>> PROCEDURE_TABLES = new HashMap<Procedure, Set<Table>>();
        /**
         * PartitionId -> Partition
         */
        public final ListOrderedMap<Integer, Partition> PARTITION_XREF = new ListOrderedMap<Integer, Partition>();
        /**
         * Host -> Set<Site>
         */
        public final Map<Host, Set<Site>> HOST_SITES = new TreeMap<Host, Set<Site>>(new CatalogFieldComparator<Host>("ipaddr"));
        /**
         * Column -> Foreign Key Parent Column
         */
        public final Map<Column, Column> FOREIGNKEY_PARENT = new HashMap<Column, Column>();
        /**
         * SiteId -> <Host, Port>
         */
        public final Map<Integer, Pair<String, Integer>> EXECUTION_SITES = new HashMap<Integer, Pair<String, Integer>>();
        
        /**
         * Construct the internal PARTITION_XREF cache map
         * @param cache
         * @param catalog_item
         */
        private synchronized void buildPartitionCache(CatalogType catalog_item) {
            Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);    
            for (Site catalog_site : catalog_clus.getSites()) {
                for (Partition catalog_part : catalog_site.getPartitions()) {
                    this.PARTITION_XREF.put(catalog_part.getId(), catalog_part);
                } // FOR
            } // FOR
        }
        
        private synchronized void buildReadOnlyColumnCache(CatalogType catalog_item) {
            Database catalog_db = CatalogUtil.getDatabase(catalog_item);
            
            Set<Column> all_modified = new HashSet<Column>();
            Set<Column> all_modified_no_inserts = new HashSet<Column>();
            
            for (Procedure catalog_proc : catalog_db.getProcedures()) {
                for (Statement catalog_stmt : catalog_proc.getStatements()) {
                    QueryType qtype = QueryType.get(catalog_stmt.getQuerytype());
                    if (qtype == QueryType.SELECT) continue;
                    
                    // Get the columns that referenced by this Statement
                    CatalogUtil.getReferencedColumns(catalog_stmt);
                    Set<Column> modified_cols = STATEMENT_MODIFIED_COLUMNS.get(catalog_stmt);
                    assert(modified_cols != null) : "Failed to get modified columns for " + catalog_stmt.fullName();
                    all_modified.addAll(modified_cols);
                    if (debug.get()) LOG.debug("ALL - " + catalog_stmt.fullName() + ": " + modified_cols);
                    if (qtype != QueryType.INSERT) {
                        all_modified_no_inserts.addAll(modified_cols);
                        if (debug.get()) LOG.debug("NOINSERT - " + catalog_stmt.fullName() + ": " + modified_cols);
                    }
                } // FOR (stmt)
            } // FOR (proc)
            
            for (Table catalog_tbl : catalog_db.getTables()) {
                Set<Column> readonly_with_inserts = new TreeSet<Column>();
                Set<Column> readonly_no_inserts = new TreeSet<Column>();
                
                for (Column catalog_col : catalog_tbl.getColumns()) {
                    // If this Column was not modified at all, then we can include it is
                    // in both READONLY_WITH_INSERTS and READONLY_NO_INSERTS
                    if (all_modified.contains(catalog_col) == false) {
                        readonly_with_inserts.add(catalog_col);
                        readonly_no_inserts.add(catalog_col);
                    // If this Column was modified but not by any non-INSERT query, then
                    // it is READONLY_NO_INSERTS
                    } else if (all_modified_no_inserts.contains(catalog_col) == false) {
                        readonly_no_inserts.add(catalog_col);
                    }
                } // FOR (col)
                
                if (debug.get()) LOG.debug(String.format("%s: READONLY_ALL%s - READONLY_NOINSERT%s",
                                                         catalog_tbl.getName(), readonly_with_inserts, readonly_no_inserts));
                
                READONLY_COLUMNS_ALL.put(catalog_tbl, readonly_with_inserts);
                READONLY_COLUMNS_NO_INSERTS.put(catalog_tbl, readonly_no_inserts);
            } // FOR (tbl)
        }
        
    }
    
    private static final Map<Database, CatalogUtil.Cache> CACHE = new HashMap<Database, CatalogUtil.Cache>();
    
    /**
     * Get the Cache handle for the Database catalog object
     * If one doesn't exist yet, it will be created 
     * @param catalog_item
     * @return
     */
    private static CatalogUtil.Cache getCatalogCache(CatalogType catalog_item) {
        final Database catalog_db = (catalog_item instanceof Database ? (Database)catalog_item : CatalogUtil.getDatabase(catalog_item));
        CatalogUtil.Cache ret = CACHE.get(catalog_db);
        if (ret == null) {
            ret = new CatalogUtil.Cache();
            CACHE.put(catalog_db, ret);
        }
        assert(ret != null) : "Failed to cache for " + catalog_item.fullName();
        return (ret);
    }
    
    public static void preload(CatalogType catalog_obj) {
        assert(catalog_obj != null);
        
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        List<PlanFragment> stmt_frags = new ArrayList<PlanFragment>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                stmt_frags.clear();
                CollectionUtil.addAll(stmt_frags, catalog_stmt.getFragments());
                CollectionUtil.addAll(stmt_frags, catalog_stmt.getMs_fragments());
                
                if (catalog_stmt.getReadonly()) {
                    for (PlanFragment catalog_frag : stmt_frags) {
                        assert(catalog_frag.getReadonly());
                        FRAGMENT_READONLY.put((long)catalog_frag.getId(), true);
                    } // FOR
                } else {
                    for (PlanFragment catalog_frag : stmt_frags) {
                        long id = (long)catalog_frag.getId();
                        FRAGMENT_READONLY.put(id, catalog_frag.getReadonly());
                    } // FOR
                }
            } // STATEMENT
        } // PROCEDURE 
    }
    

    // ------------------------------------------------------------
    // LOAD + SAVE
    // ------------------------------------------------------------
    

    /**
     * Loads a serialized catalog specification from a jar file and creates a
     * new Catalog object from it
     * 
     * @param jar_path
     * @return
     */
    public static Catalog loadCatalogFromJar(String jar_path) {
        Catalog catalog = null;
        String serializedCatalog = null;
        File file_path = new File(jar_path);
        if (debug.get()) LOG.debug("Loading catalog from jar file at '" + file_path.getAbsolutePath() + "'");
        if (!file_path.exists()) {
            LOG.error("The catalog jar file '" + jar_path + "' does not exist");
            return (null);
        }
        try {
            serializedCatalog = JarReader.readFileFromJarfile(jar_path, CatalogUtil.CATALOG_FILENAME);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        if (serializedCatalog == null) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME
                    + "' in jar file '" + jar_path + "' is null");
        } else if (serializedCatalog.isEmpty()) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME
                    + "' in jar file '" + jar_path + "' is empty");
        } else {
            catalog = new Catalog();
            if (debug.get()) LOG.debug("Extracted file '" + CatalogUtil.CATALOG_FILENAME
                                 + "' from jar file '" + jar_path + "'");
            catalog.execute(serializedCatalog);
        }
        return (catalog);
    }

    /**
     * Loads a serialized catalog specification from a text file and creates a
     * new Catalog object from it
     * 
     * @param jar_path
     * @return
     */
    public static Catalog loadCatalog(String path) {
        Catalog catalog = null;
        String serializedCatalog = null;
        try {
            serializedCatalog = FileUtil.readFile(path);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        if (serializedCatalog == null) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME
                    + "' in file '" + path + "' is null");
        } else if (serializedCatalog.isEmpty()) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME
                    + "' in file '" + path + "' is empty");
        } else {
            catalog = new Catalog();
            LOG.debug("Executing catalog from file '" + path + "'");
            catalog.execute(serializedCatalog);
        }
        return (catalog);
    }

    /**
     * 
     * @param catalog
     * @param file_path
     */
    public static void saveCatalog(Catalog catalog, String file_path) {
        File file = new File(file_path);
        try {
            FileUtil.writeStringToFile(file, catalog.serialize());
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        LOG.info("Wrote catalog contents to '" + file.getAbsolutePath() + "'");
    }
    

    private static final Map<Long, Boolean> FRAGMENT_READONLY = new HashMap<Long, Boolean>();

    /**
     * Returns true if all of the fragments in the array are read-only
     * @param catalog_obj
     * @param fragments
     * @param cnt
     * @return
     */
    public static boolean areFragmentsReadOnly(CatalogType catalog_obj, long fragments[], int cnt) {
        if (FRAGMENT_READONLY.isEmpty()) preload(catalog_obj);
        for (int i = 0; i < cnt; i++) {
            Boolean b = FRAGMENT_READONLY.get(fragments[i]);
            assert(b != null) : "Unexpected PlanFragment id #" + fragments[i];
            if (b.booleanValue() == false) return (false);
        } // FOR
        return (true);
    }
    

    /**
     * 
     * @param items
     * @return
     */
    public static Map<Object, String> getHistogramLabels(Iterable<? extends CatalogType> items) {
        Map<Object, String> labels = new HashMap<Object, String>();
        for (CatalogType ct : items) {
            labels.put(ct, CatalogUtil.getDisplayName(ct));
        } // FOR
        return (labels);
    }
    
    // ------------------------------------------------------------
    // CLUSTER + DATABASE
    // ------------------------------------------------------------
    
    /**
     * Convenience method to return the default database object from a catalog
     * @param catalog
     * @return
     */
    public static Database getDatabase(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        assert (catalog_clus != null);
        return (catalog_clus.getDatabases().get(DEFAULT_DATABASE_NAME));
    }

    /**
     * Convenience method to return the default cluster object from a catalog
     * @param catalog
     * @return
     */
    public static Cluster getCluster(CatalogType catalog_item) {
        assert (catalog_item != null) : "Null Catalog Item!";
        if (catalog_item instanceof Cluster)
            return ((Cluster) catalog_item);
        Catalog catalog = catalog_item.getCatalog();
        assert(catalog != null);
        return (catalog.getClusters().get(DEFAULT_CLUSTER_NAME));
    }
    
    // ------------------------------------------------------------
    // PROCEDURES + STATEMENTS + PARAMETERS
    // ------------------------------------------------------------
    
    /**
     * Construct a collection of all the Statements in the catalog
     * @param catalog_obj
     * @return
     */
    public static Collection<Statement> getAllStatements(CatalogType catalog_obj) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        Set<Statement> ret = new HashSet<Statement>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            ret.addAll(catalog_proc.getStatements());
        } // FOR
        return (ret);
    }

    /**
     * Returns a set of all of the PlanFragments (both multi-partition and
     * single-partition)
     * @param catalogs_stmt
     * @return
     */
    public static Set<PlanFragment> getAllPlanFragments(Statement catalog_stmt) {
        Set<PlanFragment> frags = new HashSet<PlanFragment>();
        for (PlanFragment catalog_frag : catalog_stmt.getFragments())
            frags.add(catalog_frag);
        for (PlanFragment catalog_frag : catalog_stmt.getMs_fragments())
            frags.add(catalog_frag);
        return (frags);
    }
    
    /**
     * Return the partitioning parameter used by this Procedure
     * @param catalog_proc
     * @return
     */
    public static ProcParameter getPartitioningProcParameter(Procedure catalog_proc) {
        assert (catalog_proc != null);
        ProcParameter catalog_param = null;
        if (catalog_proc.getParameters().size() > 0 && !catalog_proc.getSystemproc()) {
            int idx = catalog_proc.getPartitionparameter();
            if (idx == NullProcParameter.PARAM_IDX) {
                catalog_param = NullProcParameter.getNullProcParameter(catalog_proc);
            } else {
                catalog_param = catalog_proc.getParameters().get(idx);
                assert (catalog_param != null) : "Unexpected Null ProcParameter for "
                        + catalog_proc.getName() + " at idx #" + idx;
            }
        }
        return (catalog_param);
    }
    
    /**
     * Return an ordered list of VoltTypes for the ProcParameters for the given Procedure
     * @param catalog_proc
     * @return
     */
    public static List<VoltType> getProcParameterTypes(final Procedure catalog_proc) {
        List<VoltType> vtypes = new ArrayList<VoltType>();
        for (ProcParameter catalog_param : CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index")) {
            VoltType vtype = VoltType.get(catalog_param.getType());
            assert(vtype != null);
            assert(vtype != VoltType.INVALID);
            vtypes.add(vtype);
        } // FOR
        return (vtypes);
    }

    /**
     * Return the list of ProcParameters that are array parameters for the given procedure
     */
    public static List<ProcParameter> getArrayProcParameters(final Procedure catalog_proc) {
        List<ProcParameter> params = new ArrayList<ProcParameter>();
        for (ProcParameter catalog_param : catalog_proc.getParameters()) {
            if (catalog_param.getIsarray()) params.add(catalog_param);
        } // FOR
        return (params);
    }
    
    /**
     * Returns all the StmtParameters that are linked to the ProcParameter
     * @param catalog_stmt
     * @param catalog_proc_param
     * @return
     */
    public static Set<StmtParameter> getStmtParameters(Statement catalog_stmt, ProcParameter catalog_proc_param) {
        Set<StmtParameter> found = new HashSet<StmtParameter>();
        for (StmtParameter param : catalog_stmt.getParameters()) {
            if (param.getProcparameter() != null
                    && param.getProcparameter().equals(catalog_proc_param))
                found.add(param);
        } // FOR
        return (found);
    }
    
    // ------------------------------------------------------------
    // HOSTS + SITES + PARTITIONS
    // ------------------------------------------------------------ 

    /**
     * Return the unique Site catalog object for the given id
     * @param catalog_item
     * @return
     */
    public static Site getSiteFromId(CatalogType catalog_item, int site_id) {
        assert (site_id >= 0);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        for (Site catalog_site : catalog_clus.getSites()) {
            if (catalog_site.getId() == site_id)
                return (catalog_site);
        } // FOR
        return (null);
    }
    
    /**
     * Return the number of hosts for a catalog for any catalog item
     * @param catalog_item
     * @return
     */
    public static int getNumberOfHosts(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getHosts().size();
        assert (ret > 0);
        return (ret);
    }
    
    /**
     * Return the number of sites for a catalog for any catalog item
     * @param catalog_item
     * @return
     */
    public static int getNumberOfSites(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getSites().size();
        assert (ret > 0);
        return (ret);
    }
    
    /**
     * Return the number of partitions for a catalog for any catalog item
     * @param catalog_item
     * @return
     */
    public static int getNumberOfPartitions(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getNum_partitions();
        return (ret);
    }

    /**
     * Return a random partition id for all of the partitions available
     * @param catalog_item
     * @return
     */
    public static int getRandomPartition(CatalogType catalog_item) {
        return (rand.nextInt(CatalogUtil.getNumberOfPartitions(catalog_item)));
    }

    /**
     * Return the Partition catalog object for the given PartitionId
     * @param catalog_item
     * @param id
     * @return
     */
    public static Partition getPartitionById(CatalogType catalog_item, Integer id) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty()) cache.buildPartitionCache(catalog_item);
        Partition catalog_part = cache.PARTITION_XREF.get(id);
        return (catalog_part);
    }
    
    /**
     * Return the InetSocketAddress used by the Dtxn.Engine for the given PartitionId
     * @param catalog_item
     * @param id
     * @param engine - Whether to use the direct engine port number 
     * @return
     */
    public static InetSocketAddress getPartitionAddressById(CatalogType catalog_item, Integer id, boolean engine) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty()) cache.buildPartitionCache(catalog_item);
        Partition catalog_part = cache.PARTITION_XREF.get(id);
        if (catalog_part == null) {
            LOG.warn(String.format("Invalid partition id '%d'", id));
            return (null);
        }
        Site catalog_site = catalog_part.getParent();
        assert(catalog_site != null) : "No site for " + catalog_part; 
        Host catalog_host = catalog_site.getHost();
        assert(catalog_host != null) : "No host for " + catalog_site;
        int port = (engine ? catalog_part.getEngine_port() : catalog_part.getDtxn_port());
        return (new InetSocketAddress(catalog_host.getIpaddr(), port));
    }
    
    /**
     * Return a Collection of all the Partition catalog objects
     * @param catalog_item
     * @return
     */
    public static Collection<Partition> getAllPartitions(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty()) cache.buildPartitionCache(catalog_item);
        return (Collections.unmodifiableCollection(cache.PARTITION_XREF.values()));
    }

    /**
     * Get a new list of all the partition ids in this catalog
     * @return
     */
    public static Collection<Integer> getAllPartitionIds(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty()) cache.buildPartitionCache(catalog_item);
        return (Collections.synchronizedCollection(cache.PARTITION_XREF.asList()));
    }

    /**
     * Get a mapping of sites for each host. We have to return the Site
     * objects in order to get the Partition handle that we want
     * @return
     */
    public static Map<Host, Set<Site>> getSitesPerHost(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        final Map<Host, Set<Site>> sites = cache.HOST_SITES;
        
        if (sites.isEmpty()) {
            // Sort them by site ids
            final Comparator<Site> comparator = new Comparator<Site>() {
                @Override
                public int compare(Site o1, Site o2) {
                    return (o1.getId() - o2.getId());
                }
            };

            Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
            for (Site catalog_site : catalog_clus.getSites()) {
                Host catalog_host = catalog_site.getHost();
                if (!sites.containsKey(catalog_host)) {
                    sites.put(catalog_host, new TreeSet<Site>(comparator));
                }
                sites.get(catalog_host).add(catalog_site);
                if (debug.get()) LOG.debug(catalog_host + " => " + catalog_site);
            } // FOR
            assert(sites.size() == catalog_clus.getHosts().size());
            if (debug.get()) LOG.debug("HOST SITES: " + sites);
        }
        return (sites);
    }
    
    /**
     * Return the list of Sites for a particular host
     * @param catalog_host
     * @return
     */
    public static List<Site> getSitesForHost(Host catalog_host) {
        List<Site> sites = new ArrayList<Site>();
        Cluster cluster = (Cluster) catalog_host.getParent();
        for (Site catalog_site : cluster.getSites()) {
            if (catalog_site.getHost().getName().equals(catalog_host.getName()))
                sites.add(catalog_site);
        } // FOR
        return (sites);
    }

    /**
     * Return a mapping from SiteId to <Host, Port#>
     * @param catalog_item
     * @return
     */
    public static Map<Integer, Pair<String, Integer>> getExecutionSites(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        final Map<Integer, Pair<String, Integer>> sites = cache.EXECUTION_SITES;
        
        if (sites.isEmpty()) {
            Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
            for (Site catalog_site : CatalogUtil.getSortedCatalogItems(catalog_clus.getSites(), "id")) {
                Host catalog_host = catalog_site.getHost();
                assert (catalog_host != null);
                sites.put(catalog_site.getId(), Pair.of(catalog_host.getIpaddr(), catalog_site.getProc_port()));
            } // FOR
        }
        return (sites);
    }
    
    /**
     * Return the partition ids stored at this Site
     * @param catalog_db
     * @return
     */
    public static Set<Integer> getLocalPartitionIds(Site catalog_site) {
        Set<Integer> partition_ids = new HashSet<Integer>();
        for (Partition catalog_proc : catalog_site.getPartitions()) {
            partition_ids.add(catalog_proc.getId());
        } // FOR
        return (partition_ids);
    }
    
    /**
     * 
     * @param catalog_db
     * @param base_partition
     * @return
     */
    public static Set<Integer> getLocalPartitionIds(Database catalog_db, int base_partition) {
        Set<Integer> partition_ids = new HashSet<Integer>();
        for (Partition catalog_proc : CatalogUtil.getLocalPartitions(catalog_db, base_partition)) {
            partition_ids.add(catalog_proc.getId());
        } // FOR
        return (partition_ids);
    }

    /**
     * For a given base partition id, return the list of partitions that are on
     * same node as the base partition
     * 
     * @param catalog_db
     * @param base_partition
     * @return
     */
    public static Set<Partition> getLocalPartitions(Database catalog_db, int base_partition) {
        Set<Partition> partitions = new HashSet<Partition>();

        // First figure out what partition we are in the catalog
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_db);
        assert (catalog_clus != null);
        Partition catalog_part = CatalogUtil.getPartitionById(catalog_clus, base_partition);
        assert (catalog_part != null);
        Site catalog_site = catalog_part.getParent();
        assert(catalog_site != null);
        Host catalog_host = catalog_site.getHost();
        assert(catalog_host != null);

        // Now look at what other partitions are on the same host that we are
        for (Site other_site : catalog_clus.getSites()) {
            if (other_site.getHost().equals(catalog_host) == false) continue;
            LOG.trace(catalog_host + " => " + CatalogUtil.debug(other_site.getPartitions()));
            CollectionUtil.addAll(partitions, other_site.getPartitions());
        } // FOR
        return (partitions);
    }

    // ------------------------------------------------------------
    // TABLES + COLUMNS
    // ------------------------------------------------------------
    
    /**
     * For a given VoltTable object, return the matching Table catalog object
     * based on the column names.
     * @param catalog_db
     * @param voltTable
     * @return
     */
    public static Table getCatalogTable(Database catalog_db, VoltTable voltTable) {
        int num_columns = voltTable.getColumnCount();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (num_columns == catalog_tbl.getColumns().size()) {
                boolean match = true;
                List<Column> catalog_cols = CatalogUtil.getSortedCatalogItems(
                        catalog_tbl.getColumns(), "index");
                for (int i = 0; i < num_columns; i++) {
                    if (!voltTable.getColumnName(i).equals(
                            catalog_cols.get(i).getName())) {
                        match = false;
                        break;
                    }
                } // FOR
                if (match)
                    return (catalog_tbl);
            }
        } // FOR
        return (null);
    }

    /**
     * Return a mapping from Tables to their vertical partitions
     * @param catalog_obj
     * @return
     */
    public static Map<Table, MaterializedViewInfo> getVerticallyPartitionedTables(CatalogType catalog_obj) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        Map<Table, MaterializedViewInfo> ret = new HashMap<Table, MaterializedViewInfo>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            for (MaterializedViewInfo view : catalog_tbl.getViews()) {
                if (view.getVerticalpartition()) {
                    ret.put(catalog_tbl, view);
                    break;
                }
            } // FOR
        } // FOR
        return (ret);
    }

    /**
     * Add a new vertical partition for the given table
     * @param catalog_tbl
     * @param catalog_cols
     * @return
     */
    public static MaterializedViewInfo addVerticalPartition(Table catalog_tbl, Collection<Column> catalog_cols) {
        String name = "SYS_VP_" + catalog_tbl.getName();
        MaterializedViewInfo vp = catalog_tbl.getViews().add(name);
        vp.setVerticalpartition(true);
        vp.setDest(catalog_tbl);
        int i = 0;
        for (Column catalog_col : catalog_cols) {
            ColumnRef catalog_ref = vp.getGroupbycols().add(catalog_col.getName());
            catalog_ref.setColumn(catalog_col);
            catalog_ref.setIndex(i++);
        } // FOR
        return (vp);
    }
    
    /**
     * Return the set of read-only Columns for the given table
     * If exclude_inserts is true, then only UPDATES will be counted against columns 
     * @param catalog_tbl
     * @param exclude_inserts
     * @return
     */
    public static Set<Column> getReadOnlyColumns(Table catalog_tbl, boolean exclude_inserts) {
        assert(catalog_tbl != null);
        final CatalogUtil.Cache c = CatalogUtil.getCatalogCache(catalog_tbl);
        Set<Column> columns = (exclude_inserts ? c.READONLY_COLUMNS_NO_INSERTS : c.READONLY_COLUMNS_ALL).get(catalog_tbl);
        if (columns == null) {
            c.buildReadOnlyColumnCache(catalog_tbl);
            columns = (exclude_inserts ? c.READONLY_COLUMNS_NO_INSERTS : c.READONLY_COLUMNS_ALL).get(catalog_tbl);
        }
        return (columns);
    }
    
    /**
     * Construct a collection of all the Columns in the catalog
     * @param catalog_obj
     * @return
     */
    public static Collection<Column> getAllColumns(CatalogType catalog_obj) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        Set<Column> ret = new HashSet<Column>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            ret.addAll(catalog_tbl.getColumns());
        } // FOR
        return (ret);
    }
    
    /**
     * 
     * @param from_column
     * @return
     */
    public static Column getForeignKeyParent(Column from_column) {
        assert (from_column != null);
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(from_column);
        Column to_column = cache.FOREIGNKEY_PARENT.get(from_column);

        if (to_column == null) {
            for (Constraint catalog_const : CatalogUtil.getConstraints(from_column.getConstraints())) {
                if (catalog_const.getType() == ConstraintType.FOREIGN_KEY.getValue()) {
                    assert (!catalog_const.getForeignkeycols().isEmpty());
                    for (ColumnRef catalog_col_ref : catalog_const.getForeignkeycols()) {
                        if (catalog_col_ref.getName().equals(from_column.getName())) {
                            assert (to_column == null);
                            to_column = catalog_col_ref.getColumn();
                            break;
                        }
                    } // FOR
                    if (to_column != null) break;
                }
            } // FOR
            cache.FOREIGNKEY_PARENT.put(from_column, to_column);
        }
        return (to_column);
    }

    /**
     * Returns all the columns for this table that have a foreign key dependency
     * on another table
     * @param catalog_tbl
     * @return
     */
    public static Set<Column> getForeignKeyDependents(Table catalog_tbl) {
        Set<Column> found = new HashSet<Column>();
        for (Column catalog_col : catalog_tbl.getColumns()) {
            assert (catalog_col != null);
            if (!catalog_col.getConstraints().isEmpty()) {
                // System.out.println(catalog_col + ": " +
                // CatalogUtil.getConstraints(catalog_col.getConstraints()));
                if (!CatalogUtil.findAll(
                        CatalogUtil.getConstraints(catalog_col.getConstraints()),
                        "type",
                        ConstraintType.FOREIGN_KEY.getValue()).isEmpty()) {
                    found.add(catalog_col);
                }
            }
        } // FOR
        return (found);
    }
    
    /**
     * Return the real Constraint objects for the ConstraintRefs
     * @param map
     * @return
     */
    public static Set<Constraint> getConstraints(Iterable<ConstraintRef> map) {
        Set<Constraint> ret = new HashSet<Constraint>();
        if (map != null) {
            for (ConstraintRef ref : map) {
                Constraint catalog_item = ref.getConstraint();
                assert (catalog_item != null);
                ret.add(catalog_item);
            }
        }
        return (ret);
    }

    
    /**
     * Return the real Column objects for the ColumnRefs
     * @param map
     * @return
     */
    public static Collection<Column> getColumns(Iterable<ColumnRef> map) {
        Set<Column> ret = new HashSet<Column>();
        if (map != null) {
            for (ColumnRef ref : map) {
                Column catalog_item = ref.getColumn();
                assert (catalog_item != null);
                ret.add(catalog_item);
            }
        }
        return (ret);
    }
    
    /**
     * 
     * @param catalog_db
     * @param table_name
     * @param column_name
     * @return
     */
    public static Column getColumn(Database catalog_db, String table_name, String column_name) {
        Column catalog_col = null;
        Table catalog_table = catalog_db.getTables().get(table_name);
        if (catalog_table != null)
            catalog_col = catalog_table.getColumns().get(column_name);
        return (catalog_col);
    }

    /**
     * Returns all the tables access/modified in all the Statements for this
     * Procedure
     * 
     * @param catalog_proc
     * @return
     * @throws Exception
     */
    public static Collection<Table> getReferencedTables(Procedure catalog_proc) throws Exception {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_proc);
        Set<Table> ret = cache.PROCEDURE_TABLES.get(catalog_proc);
        if (ret == null) {
            Set<Table> tables = new HashSet<Table>();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                tables.addAll(CatalogUtil.getReferencedTables(catalog_stmt));
            } // FOR
            ret = Collections.unmodifiableSet(tables);
            cache.PROCEDURE_TABLES.put(catalog_proc, ret);
        }
        return (ret);
    }

    /**
     * Returns all the columns access/modified in all the Statements for this Procedure
     * @param catalog_proc
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumns(Procedure catalog_proc) throws Exception {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_proc);
        Set<Column> ret = cache.PROCEDURE_COLUMNS.get(catalog_proc);
        if (ret == null) {
            Set<Column> columns = new HashSet<Column>();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                columns.addAll(CatalogUtil.getReferencedColumns(catalog_stmt));
            } // FOR
            ret = Collections.unmodifiableSet(columns);
            cache.PROCEDURE_COLUMNS.put(catalog_proc, ret);
        }
        return (ret);
    }

    /**
     * Returns all of the procedures that access/modify the given table
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    public static Collection<Procedure> getReferencingProcedures(Table catalog_tbl) throws Exception {
        Set<Procedure> ret = new HashSet<Procedure>();
        Database catalog_db = CatalogUtil.getDatabase(catalog_tbl);
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) continue;
            if (CatalogUtil.getReferencedTables(catalog_proc).contains(catalog_tbl)) {
                ret.add(catalog_proc);
            }
        } // FOR
        return (ret);
    }

    /**
     * Returns all of the procedures that access/modify the given Column
     * @param catalog_col
     * @return
     * @throws Exception
     */
    public static Collection<Procedure> getReferencingProcedures(Column catalog_col) throws Exception {
        Set<Procedure> ret = new HashSet<Procedure>();
        Database catalog_db = CatalogUtil.getDatabase(catalog_col.getParent());

        // Special Case: ReplicatedColumn
        if (catalog_col instanceof ReplicatedColumn) {
            Table catalog_tbl = catalog_col.getParent();
            for (Column col : catalog_tbl.getColumns()) {
                for (Procedure catalog_proc : catalog_db.getProcedures()) {
                    if (catalog_proc.getSystemproc()) continue;
                    if (CatalogUtil.getReferencedColumns(catalog_proc).contains(col)) {
                        ret.add(catalog_proc);
                    }
                } // FOR
            } // FOR
        } else {
            for (Procedure catalog_proc : catalog_db.getProcedures()) {
                if (catalog_proc.getSystemproc()) continue;
                if (CatalogUtil.getReferencedColumns(catalog_proc).contains(catalog_col)) {
                    ret.add(catalog_proc);
                }
            } // FOR
        }
        return (ret);
    }

    /**
     * Returns all the tables access/modified in the given Statement's query
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Table> getReferencedTables(Statement catalog_stmt) throws Exception {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Set<Table> ret = cache.STATEMENT_TABLES.get(catalog_stmt);
        if (ret == null) {
            Set<Table> tables = new HashSet<Table>();
            for (Column catalog_col : CatalogUtil.getReferencedColumns(catalog_stmt)) {
                tables.add((Table)catalog_col.getParent());
            } // FOR
            ret = Collections.unmodifiableSet(tables);
            cache.STATEMENT_TABLES.put(catalog_stmt, ret);
        }
        return (ret);
    }
    
    /**
     * Get all the tables referenced in this statement
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Table> getAllTables(Statement catalog_stmt) throws Exception {
        final Database catalog_db = (Database) catalog_stmt.getParent().getParent();
        AbstractPlanNode node = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
        return (CatalogUtil.getReferencedTablesForTree(catalog_db, node));
    }

    /**
     * Returns all the columns access/modified in the given Statement's query
     * This does not include the columns that are output by SELECT queries 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumns(Statement catalog_stmt) {
        if (debug.get()) LOG.debug("Extracting table set from statement " + CatalogUtil.getDisplayName(catalog_stmt));
        
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Set<Column> ret = cache.STATEMENT_ALL_COLUMNS.get(catalog_stmt);
        if (ret == null) {
            final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
            ret = new TreeSet<Column>();
            Set<Column> modified = new TreeSet<Column>();

            // 2010-07-14: Always use the AbstractPlanNodes from the PlanFragments to figure out
            // what columns the query touches. It's more accurate because we will pick apart plan nodes
            // and expression trees to figure things out
            AbstractPlanNode node = null;
            Set<Column> columns = null;
            try {
                node = PlanNodeUtil.getPlanNodeTreeForStatement(catalog_stmt, true);
                columns = CatalogUtil.getReferencedColumnsForTree(catalog_db, node, ret, modified);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to get columns for " + catalog_stmt.fullName(), ex);
            }
            assert (columns != null) : "Failed to get columns for " + catalog_stmt.fullName();
            ret = Collections.unmodifiableSet(columns);
            cache.STATEMENT_ALL_COLUMNS.put(catalog_stmt, ret);
            cache.STATEMENT_MODIFIED_COLUMNS.put(catalog_stmt, Collections.unmodifiableSet(modified));
        }
        return (ret);
    }
    
    /**
     * Returns the set of Column catalog objects modified by the given
     * AbstractPlanNode. If you're looking for where we figure out what columns a
     * PlanNode touches that is of interest to us for figuring out how we will
     * partition things, then you've come to the right place.
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumnsForTree(final Database catalog_db, AbstractPlanNode node) throws Exception {
        return (getReferencedColumnsForTree(catalog_db, node, new TreeSet<Column>(), null));
    }
    
    private static Set<Column> getReferencedColumnsForTree(final Database catalog_db, final AbstractPlanNode node, final Set<Column> columns, final Set<Column> modified) throws Exception {
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(final AbstractPlanNode node) {
                try {
                    CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, node, columns, modified);
                } catch (Exception ex) {
                    LOG.fatal("Failed to extract columns from " + node, ex);
                    System.exit(1);
                }
                return;
            }
        }.traverse(node);
        return (columns);
    }

    /**
     * Return all of the columns referenced in the given AbstractPlanNode
     * Non-recursive.
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumnsForPlanNode(final Database catalog_db, final AbstractPlanNode node) throws Exception {
        final Set<Column> ret = new HashSet<Column>();
        CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, node, ret, null);
        return (ret);
    }
    
    /**
     * Returns all the columns referenced for the given PlanNode
     * If modified_cols is not null, then it will contain the Columns that are modified in the PlanNode
     * @param catalog_db
     * @param node
     * @param ref_columns
     * @param modified_cols
     * @throws Exception
     */
    private static void getReferencedColumnsForPlanNode(final Database catalog_db, final AbstractPlanNode node, final Set<Column> ref_columns, final Set<Column> modified_cols) throws Exception {
        switch (node.getPlanNodeType()) {
            // SCANS
            case INDEXSCAN: {
                IndexScanPlanNode idx_node = (IndexScanPlanNode) node;
                if (idx_node.getEndExpression() != null)
                    ref_columns.addAll(ExpressionUtil.getReferencedColumns(catalog_db, idx_node.getEndExpression()));
                for (AbstractExpression exp : idx_node.getSearchKeyExpressions()) {
                    if (exp != null)
                        ref_columns.addAll(ExpressionUtil.getReferencedColumns(catalog_db, exp));
                } // FOR

                // Fall through down into SEQSCAN....
            }
            case SEQSCAN: {
                AbstractScanPlanNode scan_node = (AbstractScanPlanNode) node;
                if (scan_node.getPredicate() != null)
                    ref_columns.addAll(ExpressionUtil.getReferencedColumns(catalog_db, scan_node.getPredicate()));
                break;
            }
            // JOINS
            case NESTLOOP:
            case NESTLOOPINDEX: {
                AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode) node;
                if (cast_node.getPredicate() != null)
                    ref_columns.addAll(ExpressionUtil.getReferencedColumns(catalog_db, cast_node.getPredicate()));

                // We always need to look at the inline scan nodes for joins 
                for (AbstractPlanNode inline_node : cast_node.getInlinePlanNodes().values()) {
                    if (inline_node instanceof AbstractScanPlanNode) CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, inline_node, ref_columns, modified_cols);
                }
                break;
            }
            // INSERT
            case INSERT: {
                // All columns are accessed whenever we insert a new record
                InsertPlanNode ins_node = (InsertPlanNode) node;
                Table catalog_tbl = catalog_db.getTables().get(ins_node.getTargetTableName());
                assert (catalog_tbl != null) : "Missing table " + ins_node.getTargetTableName();
                ref_columns.addAll(catalog_tbl.getColumns());
                if (modified_cols != null) modified_cols.addAll(catalog_tbl.getColumns());
                break;
            }
            // UPDATE
            case UPDATE: {
                // Need to make sure we get both the WHERE clause and the fields that are updated
                // We need to get the list of columns from the ScanPlanNode below us
                UpdatePlanNode up_node = (UpdatePlanNode) node;
                Table catalog_tbl = catalog_db.getTables().get(up_node.getTargetTableName());
                assert (catalog_tbl != null) : "Missing table " + up_node.getTargetTableName();
                
                AbstractScanPlanNode scan_node = CollectionUtil.getFirst(PlanNodeUtil.getPlanNodes(up_node, AbstractScanPlanNode.class));
                assert (scan_node != null) : "Failed to find underlying scan node for " + up_node;
                ref_columns.addAll(PlanNodeUtil.getUpdatedColumns(catalog_db, scan_node));
                if (scan_node.getInlinePlanNodeCount() > 0) {
                    ProjectionPlanNode proj_node = scan_node.getInlinePlanNode(PlanNodeType.PROJECTION);
                    assert(proj_node != null);
                    
                    // This is a bit tricky. We have to go by the names of the output columns to find what
                    // column is meant to be updated
                    PlannerContext pcontext = PlannerContext.singleton();
                    for (Integer col_guid : proj_node.getOutputColumnGUIDs()) {
                        PlanColumn pc = pcontext.get(col_guid);
                        assert(pc != null);
                        if (pc.getExpression() instanceof TupleAddressExpression) continue;
                        
                        Column catalog_col = catalog_tbl.getColumns().get(pc.displayName());
                        assert(catalog_col != null) : String.format("Missing %s.%s", catalog_tbl.getName(), pc.displayName());
                        ref_columns.add(catalog_col);
                        if (modified_cols != null) modified_cols.add(catalog_col);
                    } // FOR
                }
                break;
            }
            case DELETE:
                // I don't think we need anything here because all the
                // columns will get get picked up by the scans that feed into the DELETE
                break;
            default:
                // Do nothing...
        } // SWITCH
    }

    /**
     * Return all of the tables referenced in the given AbstractPlanNode
     * Non-recursive.
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Table> getReferencedTablesForPlanNode(final Database catalog_db, final AbstractPlanNode node) throws Exception {
        final Set<Table> ret = new HashSet<Table>();
        CatalogUtil.getReferencedTablesForPlanNode(catalog_db, node, ret);
        return (ret);
    }
    
    /**
     * Return all of tables referenced in the PlanNode tree, regardless if they
     * are modified or not
     * @param catalog_db
     * @param root
     * @return
     */
    public static Collection<Table> getReferencedTablesForTree(final Database catalog_db, final AbstractPlanNode root) {
        final Set<Table> found = new HashSet<Table>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                CatalogUtil.getReferencedTablesForPlanNode(catalog_db, element, found);
                return;
            }
        }.traverse(root);
        return (found);
    }
    
    private static void getReferencedTablesForPlanNode(final Database catalog_db, final AbstractPlanNode node, final Set<Table> found) {
        String table_name = null;
        // AbstractScanNode
        if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode cast_node = (AbstractScanPlanNode) node;
            table_name = cast_node.getTargetTableName();
            assert (table_name != null);
            assert (!table_name.isEmpty());
        // AbstractOperationPlanNode
        } else if (node instanceof AbstractOperationPlanNode) {
            AbstractOperationPlanNode cast_node = (AbstractOperationPlanNode) node;
            table_name = cast_node.getTargetTableName();
            assert (table_name != null);
            assert (!table_name.isEmpty());
        }

        if (table_name != null) {
            Table catalog_tbl = catalog_db.getTables().get(table_name);
            assert (catalog_tbl != null) : "Invalid table '"
                    + table_name + "' extracted from " + node;
            found.add(catalog_tbl);
        }
    }

    // ------------------------------------------------------------
    // COMPARATOR
    // ------------------------------------------------------------
    
    /**
     * Comparator for CatalogTypes that examines one particular field
     */
    private static final class CatalogFieldComparator<T extends CatalogType> implements Comparator<T> {
        private final String field;
        
        public CatalogFieldComparator(String field) {
            this.field = field;
        }
        
        @SuppressWarnings("unchecked")
        @Override
        public int compare(T obj0, T obj1) {
            if (obj0 == null && obj1 == null) return (0);
            if (obj0 == null) return (1);
            if (obj1 == null) return (-1);
            
            Object val0 = obj0.getField(this.field);
            Object val1 = obj1.getField(this.field);
            if (val0 == null && val1 == null) return (0);
            if (val0 == null) return (1);
            if (val1 == null) return (1);
            
            return (val0 instanceof Comparable ? ((Comparable)val0).compareTo(val1) :
                                                 val0.toString().compareTo(val1.toString()));
        };
    };
    
    // ------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------
    
    /**
     * 
     * @param <T>
     * @param <U>
     * @param items
     * @param field
     * @param value
     * @return
     */
    public static <T extends CatalogType, U> Set<T> findAll(Iterable<T> items, String field, U value) {
        Set<T> found = new HashSet<T>();
        for (T catalog_item : items) {
            assert (catalog_item != null);
            try {
                Object field_value = catalog_item.getField(field);
                if (field_value.equals(value))
                    found.add(catalog_item);
            } catch (NullPointerException ex) {
                LOG.fatal(catalog_item + ": " + catalog_item.getFields());
                LOG.fatal(catalog_item + " does not have a field '" + field + "'");
                throw ex;
            }
        } // FOR
        return (found);
    }

    /**
     * 
     * @param <T>
     * @param <U>
     * @param items
     * @param field
     * @param value
     * @return
     */
    public static <T extends CatalogType, U> T findOne(Iterable<T> items, String field, U value) {
        return (CollectionUtil.getFirst(CatalogUtil.findAll(items, field, value)));
    }

    /**
     * 
     * @param item0
     * @param item1
     * @return
     */
    public static Pair<CatalogType, CatalogType> pair(CatalogType item0, CatalogType item1) {
        Pair<CatalogType, CatalogType> pair = null;
        if (item0.compareTo(item1) < 0) {
            pair = Pair.of(item0, item1);
        } else {
            pair = Pair.of(item1, item0);
        }
        return (pair);
    }

    /**
     * 
     * @param item
     * @return
     */
    public static String getDisplayName(CatalogType item) {
        return (CatalogUtil.getDisplayName(item, false));
    }

    /**
     * 
     * @param item
     * @param include_class
     * @return
     */
    public static String getDisplayName(CatalogType item, boolean include_class) {
        if (item != null) {
            String ret = null;
            
            // Column/Statement/Constraint/Index
            // Format: <Parent>.<Item>
            if (item instanceof Column || item instanceof Statement || item instanceof Constraint || item instanceof Index) {
                ret = String.format("%s.%s", item.getParent().getName(), item.getName());
                
            // ProcParameter/StmtParameter
            // Format: <Parent>.<Item>
            } else if (item instanceof ProcParameter || item instanceof StmtParameter) {
                ret = String.format("%s.%s", item.getParent().getName(), (include_class ? item : item.getName()));
                
            // PlanFragment
            // Format: <Procedure>.<Statement>.[Fragment #XYZ]
            } else if (item instanceof PlanFragment) {
                ret = String.format("%s.%s.[Fragment #%s]", item.getParent().getParent().getName(),
                                                            item.getParent().getName(),
                                                            item.getName());
            
            // ConstantValue
            // Format: ConstantValue{XYZ}
            } else if (item instanceof ConstantValue) {
                ret = String.format("%s{%s}", item.getClass().getSimpleName(), ((ConstantValue) item).getValue());
                
            // Everything Else
            // Format: <OptionalClassName>.<Item>
            } else {
                ret = String.format("%s%s", (include_class ? item.getClass().getSimpleName() + ":" : ""),
                                            item.getName());
            }
            return (ret);
        }
        return (null);
    }

    /**
     * 
     * @param catalog_stmt
     * @return
     */
    public static String debugJSON(Statement catalog_stmt) {
        String jsonString = Encoder.hexDecodeToString(catalog_stmt
                .getFullplan());
        String line = "\n----------------------------------------\n";
        String ret = "FULL PLAN ORIG STRING:\n" + jsonString + line;

        for (PlanFragment catalog_frgmt : catalog_stmt.getFragments()) {
            jsonString = Encoder.hexDecodeToString(catalog_frgmt
                    .getPlannodetree());
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                ret += "FRAGMENT " + catalog_frgmt.getName() + "\n"
                        + jsonObject.toString(2) + line;
            } catch (Exception ex) {
                ex.printStackTrace();
                System.exit(1);
            }
        } // FOR
        return (ret);
    }

    /**
     * Return a string representation of this CatalogType handle
     * 
     * @param catalog_item
     * @return
     */
    public static String debug(CatalogType catalog_item) {
        StringBuilder buffer = new StringBuilder();
        buffer.append(catalog_item.toString()).append("\n");
        Set<String> fields = new HashSet<String>();
        fields.addAll(catalog_item.getFields());
        fields.addAll(catalog_item.getChildFields());

        for (String field : fields) {
            String value = null;
            if (catalog_item.getChildFields().contains(field)) {
                value = CatalogUtil.debug(catalog_item.getChildren(field));
            } else {
                value = catalog_item.getField(field).toString();
            }

            buffer.append("  ").append(String.format("%-20s", field + ":"))
                    .append(value).append("\n");
        } // FOR
        return (buffer.toString());
    }
    
    public static String debug(CatalogMap<? extends CatalogType> map) {
        String ret = "";
        String add = "";
        for (CatalogType item : map) {
            ret += add + item;
            add = ", ";
        } // FOR
        return ("[" + ret + "]");
    }

    public static String debug(Collection<? extends CatalogType> items) {
        if (items == null) return (null);
        String ret = "";
        String add = "";
        for (CatalogType item : items) {
            if (item != null) {
                ret += add + CatalogUtil.getDisplayName(item);
                add = ", ";
            }
        } // FOR
        return ("[" + ret + "]");
    }
    
} // END CLASS