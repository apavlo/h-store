package edu.brown.catalog;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.json.JSONObject;
import org.voltdb.CatalogContext;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.ConstantValue;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.compiler.JarBuilder;
import org.voltdb.expressions.AbstractExpression;
import org.voltdb.expressions.AbstractValueExpression;
import org.voltdb.expressions.ComparisonExpression;
import org.voltdb.expressions.ConjunctionExpression;
import org.voltdb.expressions.ConstantValueExpression;
import org.voltdb.expressions.InComparisonExpression;
import org.voltdb.expressions.ParameterValueExpression;
import org.voltdb.expressions.TupleAddressExpression;
import org.voltdb.expressions.TupleValueExpression;
import org.voltdb.planner.PlanColumn;
import org.voltdb.planner.PlannerContext;
import org.voltdb.plannodes.AbstractJoinPlanNode;
import org.voltdb.plannodes.AbstractOperationPlanNode;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.plannodes.AbstractScanPlanNode;
import org.voltdb.plannodes.IndexScanPlanNode;
import org.voltdb.plannodes.InsertPlanNode;
import org.voltdb.plannodes.MaterializePlanNode;
import org.voltdb.plannodes.NestLoopIndexPlanNode;
import org.voltdb.plannodes.OrderByPlanNode;
import org.voltdb.plannodes.ProjectionPlanNode;
import org.voltdb.plannodes.SeqScanPlanNode;
import org.voltdb.plannodes.UpdatePlanNode;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.ExpressionType;
import org.voltdb.types.PlanNodeType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Encoder;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.Pair;

import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.catalog.special.SpecialProcParameter;
import edu.brown.expressions.ExpressionTreeWalker;
import edu.brown.expressions.ExpressionUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeTreeWalker;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;

/**
 * @author pavlo
 */
public abstract class CatalogUtil extends org.voltdb.utils.CatalogUtil {
    private static final Logger LOG = Logger.getLogger(CatalogUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public static final String DEFAULT_CLUSTER_NAME = "cluster";
    public static final String DEFAULT_DATABASE_NAME = "database";
    public static final String DEFAULT_PROCEDURE_NAME = "procedure";
    public static final String DEFAULT_STATEMENT_NAME = "statement";

    // ------------------------------------------------------------
    // CACHES
    // ------------------------------------------------------------

    @Deprecated
    private static class Cache {

        /**
         * The set of read-only columns excluding inserts
         */
        private final Map<Table, Set<Column>> READONLY_COLUMNS_NO_INSERTS = new HashMap<Table, Set<Column>>();
        /**
         * The set of read-only columns including inserts
         */
        private final Map<Table, Set<Column>> READONLY_COLUMNS_ALL = new HashMap<Table, Set<Column>>();
        /**
         * The set of Columns referenced for each Statement Statement ->
         * Set<Column>
         */
        private final Map<Statement, Set<Column>> STATEMENT_ALL_COLUMNS = new HashMap<Statement, Set<Column>>();
        /**
         * The set of Columns referenced modified in each Statement Statement ->
         * Set<Column>
         */
        private final Map<Statement, Set<Column>> STATEMENT_MODIFIED_COLUMNS = new HashMap<Statement, Set<Column>>();
        /**
         * The set of Columns that read-only in each Statement Statement ->
         * Set<Column>
         */
        private final Map<Statement, Collection<Column>> STATEMENT_READONLY_COLUMNS = new HashMap<Statement, Collection<Column>>();
        /**
         * The set of Columns that are used in the ORDER BY clause of the query
         * Statement -> Set<Column>
         */
        private final Map<Statement, Set<Column>> STATEMENT_ORDERBY_COLUMNS = new HashMap<Statement, Set<Column>>();
        /**
         * Statement -> Set<Table>
         */
        private final Map<Statement, Collection<Table>> STATEMENT_TABLES = new HashMap<Statement, Collection<Table>>();
        /**
         * Procedure -> Set<Column>
         */
        private final Map<Procedure, Set<Column>> PROCEDURE_COLUMNS = new HashMap<Procedure, Set<Column>>();
        /**
         * Procedure -> Set
         * <Table>
         */
        private final Map<Procedure, Set<Table>> PROCEDURE_TABLES = new HashMap<Procedure, Set<Table>>();
        /**
         * PartitionId -> Partition
         */
        private final ListOrderedMap<Integer, Partition> PARTITION_XREF = new ListOrderedMap<Integer, Partition>();
        /**
         * Host -> Set<Site>
         */
        private final Map<Host, Set<Site>> HOST_SITES = new TreeMap<Host, Set<Site>>(new CatalogFieldComparator<Host>("ipaddr"));
        /**
         * Column -> Foreign Key Parent Column
         */
        private final Map<Column, Column> FOREIGNKEY_PARENT = new HashMap<Column, Column>();
        /**
         * SiteId -> Set<<Host, Port>>
         */
        private final Map<Integer, Set<Pair<String, Integer>>> EXECUTION_SITES = new HashMap<Integer, Set<Pair<String, Integer>>>();

        private final Map<Pair<String, Collection<String>>, PredicatePairs> EXTRACTED_PREDICATES = new HashMap<Pair<String, Collection<String>>, PredicatePairs>();

        /**
         * Construct the internal PARTITION_XREF cache map
         * 
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
                    if (qtype == QueryType.SELECT)
                        continue;

                    // Get the columns that referenced by this Statement
                    CatalogUtil.getReferencedColumns(catalog_stmt);
                    Set<Column> modified_cols = STATEMENT_MODIFIED_COLUMNS.get(catalog_stmt);
                    assert (modified_cols != null) : "Failed to get modified columns for " + catalog_stmt.fullName();
                    all_modified.addAll(modified_cols);
                    if (debug.val)
                        LOG.debug("ALL - " + catalog_stmt.fullName() + ": " + modified_cols);
                    if (qtype != QueryType.INSERT) {
                        all_modified_no_inserts.addAll(modified_cols);
                        if (debug.val)
                            LOG.debug("NOINSERT - " + catalog_stmt.fullName() + ": " + modified_cols);
                    }
                } // FOR (stmt)
            } // FOR (proc)

            for (Table catalog_tbl : catalog_db.getTables()) {
                Set<Column> readonly_with_inserts = new TreeSet<Column>();
                Set<Column> readonly_no_inserts = new TreeSet<Column>();

                for (Column catalog_col : catalog_tbl.getColumns()) {
                    // If this Column was not modified at all, then we can
                    // include it is
                    // in both READONLY_WITH_INSERTS and READONLY_NO_INSERTS
                    if (all_modified.contains(catalog_col) == false) {
                        readonly_with_inserts.add(catalog_col);
                        readonly_no_inserts.add(catalog_col);
                        // If this Column was modified but not by any non-INSERT
                        // query, then
                        // it is READONLY_NO_INSERTS
                    } else if (all_modified_no_inserts.contains(catalog_col) == false) {
                        readonly_no_inserts.add(catalog_col);
                    }
                } // FOR (col)

                if (debug.val)
                    LOG.debug(String.format("%s: READONLY_ALL%s - READONLY_NOINSERT%s", catalog_tbl.getName(), readonly_with_inserts, readonly_no_inserts));

                READONLY_COLUMNS_ALL.put(catalog_tbl, readonly_with_inserts);
                READONLY_COLUMNS_NO_INSERTS.put(catalog_tbl, readonly_no_inserts);
            } // FOR (tbl)
        }

    }

    private static final Map<Database, CatalogUtil.Cache> CACHE = new HashMap<Database, CatalogUtil.Cache>();

    /**
     * Get the Cache handle for the Database catalog object If one doesn't exist
     * yet, it will be created
     * 
     * @param catalog_item
     * @return
     */
    private static CatalogUtil.Cache getCatalogCache(CatalogType catalog_item) {
        final Database catalog_db = (catalog_item instanceof Database ? (Database) catalog_item : CatalogUtil.getDatabase(catalog_item));
        CatalogUtil.Cache ret = CACHE.get(catalog_db);
        if (ret == null) {
            ret = new CatalogUtil.Cache();
            CACHE.put(catalog_db, ret);
        }
        assert (ret != null) : "Failed to cache for " + catalog_item.fullName();
        return (ret);
    }

    public static void preload(CatalogType catalog_obj) {
        assert (catalog_obj != null);
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);

        // Pre-load all the arrays for the CatalogMaps that we will access
        // frequently
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            catalog_proc.getStatements().values();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                catalog_stmt.getFragments().values();
                catalog_stmt.getMs_fragments().values();
            } // STATEMENT
        } // PROCEDURE
    }

    public static void clearCache(CatalogType catalog_obj) {
        assert (catalog_obj != null);
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        CACHE.remove(catalog_db);
    }

    // ------------------------------------------------------------
    // COMPARATOR
    // ------------------------------------------------------------

    /**
     * Loads a serialized catalog specification from a jar file and creates a
     * new CatalogContext object from it
     * @param jar_path
     */
    public static CatalogContext loadCatalogContextFromJar(File jar_path) {
        Catalog catalog = CatalogUtil.loadCatalogFromJar(jar_path);
        return new CatalogContext(catalog, jar_path);
    }
    
    /**
     * Loads a serialized catalog specification from a jar file and creates a
     * new Catalog object from it
     * 
     * @param jar_path
     * @return
     */
    @Deprecated
    public static Catalog loadCatalogFromJar(String jar_path) {
        return loadCatalogFromJar(new File(jar_path));
    }
    
    /**
     * Loads a serialized catalog specification from a jar file and creates a
     * new Catalog object from it
     * 
     * @param jar_path
     * @return
     */
    public static Catalog loadCatalogFromJar(File jar_path) {
        assert(jar_path != null) : "Unexpected null jar path";
        if (debug.val)
            LOG.debug("Loading catalog from jar file at '" + jar_path.getAbsolutePath() + "'");
        if (!jar_path.exists()) {
            LOG.error("The catalog jar file '" + jar_path + "' does not exist");
            return (null);
        }
        
        Catalog catalog = null;
        String serializedCatalog = null;
        try {
            serializedCatalog = JarReader.readFileFromJarfile(jar_path.getAbsolutePath(), CatalogUtil.CATALOG_FILENAME);
        } catch (Exception ex) {
            ex.printStackTrace();
            return (null);
        }
        if (serializedCatalog == null) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME + "' in jar file '" + jar_path + "' is null");
        } else if (serializedCatalog.isEmpty()) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME + "' in jar file '" + jar_path + "' is empty");
        } else {
            catalog = new Catalog();
            if (debug.val)
                LOG.debug("Extracted file '" + CatalogUtil.CATALOG_FILENAME + "' from jar file '" + jar_path + "'");
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
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME + "' in file '" + path + "' is null");
        } else if (serializedCatalog.isEmpty()) {
            LOG.warn("The catalog file '" + CatalogUtil.CATALOG_FILENAME + "' in file '" + path + "' is empty");
        } else {
            catalog = new Catalog();
            LOG.debug("Executing catalog from file '" + path + "'");
            catalog.execute(serializedCatalog);
        }
        return (catalog);
    }

    /**
     * @param catalog
     * @param file_path
     */
    public static File saveCatalog(Catalog catalog, String file_path) {
        File file = new File(file_path);
        try {
            FileUtil.writeStringToFile(file, catalog.serialize());
        } catch (Exception ex) {
            ex.printStackTrace();
            throw new RuntimeException(ex);
        }
        LOG.info("Wrote catalog contents to '" + file.getAbsolutePath() + "'");
        return (file);
    }

    /**
     * Updates the catalog file in the Jar.
     * 
     * @param jarFileName
     * @param catalog
     * @throws Exception (VoltCompilerException DNE?)
     */
    public static void updateCatalogInJar(File jarFileName, Catalog catalog, File...additions) throws Exception {
        catalog.serialize();
        // Read the old jar file into memory with JarReader.
        JarReader reader = new JarReader(jarFileName.getAbsolutePath());
        List<String> files = reader.getContentsFromJarfile();
        ArrayList<byte[]> bytes = new ArrayList<byte[]>();
        for (String file : files) {
            bytes.add(JarReader.readFileFromJarAtURL(jarFileName.getAbsolutePath(), file));
        }
        
        // Write everything from the old jar except the catalog to the same 
        // file with JarBuilder.
        JarBuilder builder = new JarBuilder(null);
        for (int i = 0; i < files.size(); ++i) {
            String file = files.get(i);
            if (file.equals(CatalogUtil.CATALOG_FILENAME)) {
                builder.addEntry(CatalogUtil.CATALOG_FILENAME,
                                 catalog.serialize().getBytes());
            }
            else {
                builder.addEntry(file, bytes.get(i));
            }
        }
        
        // Add any additions that they want to the root of the the jar structure
        for (File f : additions) {
            if (f != null) {
                builder.addEntry(f.getName(), FileUtil.readBytesFromFile(f.getAbsolutePath()));
            }
        } // FOR
        
        builder.writeJarToDisk(jarFileName.getAbsolutePath());
    }
    
    /**
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
     * 
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
     * 
     * @param catalog
     * @return
     */
    public static Cluster getCluster(CatalogType catalog_item) {
        assert (catalog_item != null) : "Null Catalog Item!";
        if (catalog_item instanceof Cluster)
            return ((Cluster) catalog_item);
        Catalog catalog = catalog_item.getCatalog();
        assert (catalog != null) : catalog_item + " is missing its catalog?";
        return (catalog.getClusters().get(DEFAULT_CLUSTER_NAME));
    }

    // ------------------------------------------------------------
    // PROCEDURES + STATEMENTS + PARAMETERS
    // ------------------------------------------------------------

    /**
     * Return all of the internal system Procedures for the database
     */
    @Deprecated
    public static Collection<Procedure> getSysProcedures(Database catalog_db) {
        Collection<Procedure> procs = new ArrayList<Procedure>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc()) {
                assert(procs.contains(catalog_proc) == false);
                procs.add(catalog_proc);
            }
        }
        return (procs);
    }

    /**
     * Return all of the MapReduce Procedures for the database
     */
    @Deprecated
    public static Collection<Procedure> getMapReduceProcedures(Database catalog_db) {
        Collection<Procedure> procs = new ArrayList<Procedure>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getMapreduce())
                procs.add(catalog_proc);
        }
        return (procs);
    }

    /**
     * Construct a collection of all the Statements in the catalog
     * 
     * @param catalog_obj
     * @return
     */
    public static Collection<Statement> getAllStatements(CatalogType catalog_obj) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        Collection<Statement> ret = new ArrayList<Statement>();
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            ret.addAll(catalog_proc.getStatements());
        } // FOR
        return (ret);
    }
    
    /**
     * Construct a collection of all prefetchable Statements for a Procedure
     * @param catalog_proc
     * @return
     */
    public static Collection<Statement> getPrefetchableStatements(Procedure catalog_proc) {
        Collection<Statement> ret = new ArrayList<Statement>();
        for (Statement catalog_stmt : catalog_proc.getStatements()) {
            if (catalog_stmt.getPrefetchable()) ret.add(catalog_stmt);
        } // FOR
        return (ret);
    }

    /**
     * Returns a set of all of the PlanFragments (both multi-partition and
     * single-partition)
     * 
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
     * 
     * @param catalog_proc
     * @return
     */
    public static ProcParameter getPartitioningProcParameter(Procedure catalog_proc) {
        assert (catalog_proc != null);
        ProcParameter catalog_param = null;
        if (catalog_proc.getParameters().size() > 0 && !catalog_proc.getSystemproc()) {
            int idx = catalog_proc.getPartitionparameter();
            if (idx == NullProcParameter.PARAM_IDX) {
                catalog_param = NullProcParameter.singleton(catalog_proc);
            } else {
                catalog_param = catalog_proc.getParameters().get(idx);
                assert (catalog_param != null) : "Unexpected Null ProcParameter for " + catalog_proc.getName() + " at idx #" + idx;
            }
        }
        return (catalog_param);
    }

    /**
     * Return an ordered list of VoltTypes for the ProcParameters for the given
     * Procedure
     * 
     * @param catalog_proc
     * @return
     */
    public static List<VoltType> getProcParameterTypes(final Procedure catalog_proc) {
        List<VoltType> vtypes = new ArrayList<VoltType>();
        for (ProcParameter catalog_param : CatalogUtil.getSortedCatalogItems(catalog_proc.getParameters(), "index")) {
            VoltType vtype = VoltType.get(catalog_param.getType());
            assert (vtype != null);
            assert (vtype != VoltType.INVALID);
            vtypes.add(vtype);
        } // FOR
        return (vtypes);
    }

    /**
     * Return the list of ProcParameters that are array parameters for the given
     * procedure
     */
    public static List<ProcParameter> getArrayProcParameters(final Procedure catalog_proc) {
        List<ProcParameter> params = new ArrayList<ProcParameter>();
        for (ProcParameter catalog_param : catalog_proc.getParameters()) {
            if (catalog_param.getIsarray())
                params.add(catalog_param);
        } // FOR
        return (params);
    }

    /**
     * Return the list of every ProcParameter except for SpecialProcParameters
     */
    public static List<ProcParameter> getRegularProcParameters(final Procedure catalog_proc) {
        List<ProcParameter> params = new ArrayList<ProcParameter>();
        for (ProcParameter catalog_param : catalog_proc.getParameters()) {
            if (catalog_param instanceof SpecialProcParameter)
                continue;
            params.add(catalog_param);
        } // FOR
        return (params);
    }

    /**
     * Returns all the StmtParameters that are linked to the ProcParameter
     * 
     * @param catalog_stmt
     * @param catalog_proc_param
     * @return
     */
    public static Set<StmtParameter> getStmtParameters(Statement catalog_stmt, ProcParameter catalog_proc_param) {
        Set<StmtParameter> found = new HashSet<StmtParameter>();
        for (StmtParameter param : catalog_stmt.getParameters()) {
            if (param.getProcparameter() != null && param.getProcparameter().equals(catalog_proc_param))
                found.add(param);
        } // FOR
        return (found);
    }

    /**
     * Copy the query plans from Statement to another.
     * This will overwrite both the existing single-partition and
     * multi-partition query plans.
     * @param copy_src
     * @param copy_dest
     */
    public static void copyQueryPlans(Statement copy_src, Statement copy_dest) {
        // Update both the single and multi-partition query plans
        for (boolean sp : new boolean[] { true, false }) {
            if (debug.val)
                LOG.debug(String.format("Copying %s-partition query plan from %s to %s", (sp ? "single" : "multi"), copy_src.fullName(), copy_dest.fullName()));
            String fields[] = { (sp ? "" : "ms_") + "exptree", (sp ? "" : "ms_") + "fullplan", "has_" + (sp ? "single" : "multi") + "sited", };
            copyFields(copy_src, copy_dest, fields);

            CatalogMap<PlanFragment> copy_src_fragments = null;
            CatalogMap<PlanFragment> copy_dest_fragments = null;
            if (sp) {
                copy_src_fragments = copy_src.getFragments();
                copy_dest_fragments = copy_dest.getFragments();
            } else {
                copy_src_fragments = copy_src.getMs_fragments();
                copy_dest_fragments = copy_dest.getMs_fragments();
            }
            assert (copy_src_fragments != null);
            assert (copy_dest_fragments != null);

            copy_dest_fragments.clear();
            for (PlanFragment copy_src_frag : copy_src_fragments) {
                PlanFragment copy_dest_frag = copy_dest_fragments.add(copy_src_frag.getName());
                if (trace.val)
                    LOG.trace(String.format("Copying %s to %s", copy_src_frag.fullName(), copy_dest_frag.fullName()));
                copyFields(copy_src_frag, copy_dest_frag, copy_src_frag.getFields().toArray(new String[0]));
            } // FOR
        }
    }

    private static <T extends CatalogType> void copyFields(T copy_src, T copy_dest, String... fields) {
        for (String f : fields) {
            Object src_val = copy_src.getField(f);
            if (src_val != null) {
                if (src_val instanceof String)
                    src_val = "\"" + src_val + "\""; // HACK
                if (trace.val)
                    LOG.trace(String.format("Copied value '%s' for field '%s': %s => %s", src_val, f, copy_dest.fullName(), copy_src.fullName()));
                copy_dest.set(f, src_val.toString());
            } else if (debug.val) {
                LOG.warn(String.format("Missing value for field '%s': %s => %s", f, copy_dest.fullName(), copy_src.fullName()));
            }
        } // FOR
    }

    // ------------------------------------------------------------
    // HOSTS + SITES + PARTITIONS
    // ------------------------------------------------------------

    /**
     * Return the unique Site catalog object for the given id
     * 
     * @param catalog_item
     * @return
     */
    @Deprecated
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
     * 
     * @param catalog_item
     * @return
     */
    @Deprecated
    public static int getNumberOfHosts(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getHosts().size();
        assert (ret > 0);
        return (ret);
    }

    /**
     * Return the number of sites for a catalog for any catalog item
     * 
     * @param catalog_item
     * @return
     */
    @Deprecated
    public static int getNumberOfSites(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getSites().size();
        assert (ret > 0);
        return (ret);
    }

    /**
     * Return the number of partitions for a catalog for any catalog item
     * 
     * @param catalog_item
     * @return
     */
    @Deprecated
    public static int getNumberOfPartitions(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        int ret = catalog_clus.getNum_partitions();
        return (ret);
    }

    /**
     * Return the Partition catalog object for the given PartitionId
     * 
     * @param catalog_item
     * @param id
     * @return
     */
    @Deprecated
    public static Partition getPartitionById(CatalogType catalog_item, Integer id) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty())
            cache.buildPartitionCache(catalog_item);
        Partition catalog_part = cache.PARTITION_XREF.get(id);
        return (catalog_part);
    }
    
    /**
     * Return a Collection of all the Partition catalog objects
     * 
     * @param catalog_item
     * @return
     */
    public static Collection<Partition> getAllPartitions(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty())
            cache.buildPartitionCache(catalog_item);
        return (Collections.unmodifiableCollection(cache.PARTITION_XREF.values()));
    }

    /**
     * Get a new list of all the partition ids in this catalog
     * 
     * @return
     */
    @Deprecated
    public static PartitionSet getAllPartitionIds(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        if (cache.PARTITION_XREF.isEmpty())
            cache.buildPartitionCache(catalog_item);
        return (new PartitionSet(cache.PARTITION_XREF.asList()));
    }

    /**
     * Returns true if the given Partition is the "first" one at the given Site
     * 
     * @param catalog_site
     * @param catalog_part
     * @return
     */
    public static boolean isFirstPartition(Site catalog_site, Partition catalog_part) {
        for (Partition p : getSortedCatalogItems(catalog_site.getPartitions(), "id")) {
            if (p.getId() == catalog_part.getId())
                return (true);
            break;
        }
        return (false);
    }

    public static Collection<Site> getAllSites(CatalogType catalog_item) {
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
        return (catalog_clus.getSites());
    }

    /**
     * Get a mapping of sites for each host. We have to return the Site objects
     * in order to get the Partition handle that we want
     * 
     * @return
     */
    public static synchronized Map<Host, Set<Site>> getSitesPerHost(CatalogType catalog_item) {
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
                if (debug.val)
                    LOG.debug(catalog_host + " => " + catalog_site);
            } // FOR
            assert (sites.size() == catalog_clus.getHosts().size());
            if (debug.val)
                LOG.debug("HOST SITES: " + sites);
        }
        return (sites);
    }

    /**
     * Get a reverse mapping from PartitionId -> SiteID
     * @param catalog_item
     * @return
     */
    @Deprecated
    public static Map<Integer, Integer> getPartitionSiteXref(CatalogType catalog_item) {
        Map<Integer, Integer> m = new HashMap<Integer, Integer>();
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_item)) {
            Site catalog_site = catalog_part.getParent();
            m.put(catalog_part.getId(), catalog_site.getId());
        } // FOR
        return (m);
    }
    
    /**
     * Get a reverse mapping array from PartitionId -> SiteID
     * @param catalog_item
     * @return
     */
    public static int[] getPartitionSiteXrefArray(CatalogType catalog_item) {
        int partition_site_xref[] = new int[CatalogUtil.getNumberOfPartitions(catalog_item)];
        for (Partition catalog_part : CatalogUtil.getAllPartitions(catalog_item)) {
            partition_site_xref[catalog_part.getId()] = ((Site)catalog_part.getParent()).getId();
        } // FOR
        return (partition_site_xref);
    }

    /**
     * Return the list of Sites for a particular host
     * 
     * @param catalog_host
     * @return
     */
    public static Collection<Site> getSitesForHost(Host catalog_host) {
        List<Site> sites = new ArrayList<Site>();
        Cluster cluster = (Cluster) catalog_host.getParent();
        for (Site catalog_site : cluster.getSites()) {
            if (catalog_site.getHost().getName().equals(catalog_host.getName()))
                sites.add(catalog_site);
        } // FOR
        // Sort them by id
        Collections.sort(sites, new CatalogFieldComparator<Site>("id"));
        return (sites);
    }
    
    /**
     * Return the list of Partition for a particular host
     * 
     * @param catalog_host
     * @return
     */
    public static Collection<Partition> getPartitionsForHost(Host catalog_host) {
        List<Partition> partitions = new ArrayList<Partition>();
        Cluster cluster = (Cluster) catalog_host.getParent();
        for (Site catalog_site : cluster.getSites()) {
            if (catalog_site.getHost().getName().equals(catalog_host.getName()))
                partitions.addAll(catalog_site.getPartitions());
        } // FOR
        return (partitions);
    }

    /**
     * Return a mapping from SiteId to <Host, Port#>
     * 
     * @param catalog_item
     * @return
     */
    public static Map<Integer, Set<Pair<String, Integer>>> getExecutionSites(CatalogType catalog_item) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_item);
        final Map<Integer, Set<Pair<String, Integer>>> sites = cache.EXECUTION_SITES;

        if (sites.isEmpty()) {
            Cluster catalog_clus = CatalogUtil.getCluster(catalog_item);
            for (Site catalog_site : CatalogUtil.getSortedCatalogItems(catalog_clus.getSites(), "id")) {
                Host catalog_host = catalog_site.getHost();
                assert (catalog_host != null);
                Set<Pair<String, Integer>> s = new HashSet<Pair<String, Integer>>();
                for (Integer port : CatalogUtil.getExecutionSitePorts(catalog_site)) {
                    s.add(Pair.of(catalog_host.getIpaddr(), port));
                    // break;
                } // FOR
                sites.put(catalog_site.getId(), s);
            } // FOR
        }
        return (sites);
    }

    public static Collection<Integer> getExecutionSitePorts(Site catalog_site) {
        return Collections.singleton(catalog_site.getProc_port());
    }

    /**
     * Return the partition ids stored at this Site
     * 
     * @param catalogContext
     * @return
     */
    public static PartitionSet getLocalPartitionIds(Site catalog_site) {
        PartitionSet partition_ids = new PartitionSet();
        for (Partition catalog_proc : catalog_site.getPartitions()) {
            partition_ids.add(catalog_proc.getId());
        } // FOR
        return (partition_ids);
    }

    /**
     * @param catalog_db
     * @param base_partition
     * @return
     */
    public static PartitionSet getLocalPartitionIds(Database catalog_db, int base_partition) {
        PartitionSet partition_ids = new PartitionSet();
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
    public static Collection<Partition> getLocalPartitions(Database catalog_db, int base_partition) {
        Set<Partition> partitions = new ListOrderedSet<Partition>();

        // First figure out what partition we are in the catalog
        Cluster catalog_clus = CatalogUtil.getCluster(catalog_db);
        assert (catalog_clus != null);
        Partition catalog_part = CatalogUtil.getPartitionById(catalog_clus, base_partition);
        assert (catalog_part != null);
        Site catalog_site = catalog_part.getParent();
        assert (catalog_site != null);
        Host catalog_host = catalog_site.getHost();
        assert (catalog_host != null);

        // Now look at what other partitions are on the same host that we are
        for (Site other_site : catalog_clus.getSites()) {
            if (other_site.getHost().equals(catalog_host) == false)
                continue;
            LOG.trace(catalog_host + " => " + CatalogUtil.debug(other_site.getPartitions()));
            CollectionUtil.addAll(partitions, other_site.getPartitions());
        } // FOR
        return (partitions);
    }

    // ------------------------------------------------------------
    // TABLES + COLUMNS
    // ------------------------------------------------------------

    /**
     * Return the tables encoded in the given TableRefs
     * @param tableRefs
     * @return
     */
    public static Collection<Table> getTablesFromRefs(Collection<TableRef> tableRefs) {
        List<Table> tables = new ArrayList<Table>();
        for (TableRef ref : tableRefs) {
            tables.add(ref.getTable());
        } // FOR
        return (tables);
    }
    
    /**
     * Return all of the internal system tables for the database
     */
    public static Collection<Table> getSysTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                tables.add(catalog_tbl);
        }
        return (tables);
    }

    /**
     * Return all of the user-defined data tables for the database
     */
    public static Collection<Table> getDataTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable() == false &&
                catalog_tbl.getMapreduce() == false &&
                catalog_tbl.getMaterializer() == null)
                tables.add(catalog_tbl);
        }
        return (tables);
    }
    
    /**
     * Return all of the materialized view tables for the database
     */
    public static Collection<Table> getViewTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getMaterializer() != null)
                tables.add(catalog_tbl);
        }
        return (tables);
    }

    /**
     * Return all of the MapReduce input data tables for the database
     */
    public static Collection<Table> getMapReduceTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getMapreduce())
                tables.add(catalog_tbl);
        }
        return (tables);
    }
    
    /**
     * Return all of the replicated tables for the database
     */
    public static Collection<Table> getReplicatedTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getIsreplicated()) tables.add(catalog_tbl);
        }
        return (tables);
    }
    
    /**
     * Return all of the evictable tables for the database
     */
    public static Collection<Table> getEvictableTables(Database catalog_db) {
        List<Table> tables = new ArrayList<Table>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getEvictable()) tables.add(catalog_tbl);
        }
        return (tables);
    }

    /**
     * For a given VoltTable object, return the matching Table catalog object
     * based on the column names.
     * 
     * @param catalog_db
     * @param voltTable
     * @return
     */
    public static Table getCatalogTable(Database catalog_db, VoltTable voltTable) {
        int num_columns = voltTable.getColumnCount();
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (num_columns == catalog_tbl.getColumns().size()) {
                boolean match = true;
                List<Column> catalog_cols = CatalogUtil.getSortedCatalogItems(catalog_tbl.getColumns(), "index");
                for (int i = 0; i < num_columns; i++) {
                    if (!voltTable.getColumnName(i).equals(catalog_cols.get(i).getName())) {
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
     * Return a set of all the names of the tables in the catalog This set will
     * exclude system table names
     * 
     * @param catalog_obj
     */
    public static Collection<String> getAllTableNames(CatalogType catalog_obj) {
        return (CatalogUtil.getAllTableNames(catalog_obj, false));
    }

    /**
     * Return a set of all the names of the tables in the catalog If include_sys
     * is set to true, then the output will include system table names
     * Otherwise, they we will be excluded
     * 
     * @param catalog_obj
     * @param include_sys
     * @return
     */
    public static Collection<String> getAllTableNames(CatalogType catalog_obj, boolean include_sys) {
        Set<String> tableNames = new HashSet<String>();
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        for (Table catalog_tbl : catalog_db.getTables()) {
            boolean is_systable = catalog_tbl.getSystable();
            if (include_sys || is_systable == false)
                tableNames.add(catalog_tbl.getName());
        } // FOR
        return (tableNames);
    }

    /**
     * Return a mapping from Tables to their vertical partitions
     * 
     * @param catalog_obj
     */
    public static Map<Table, MaterializedViewInfo> getVerticallyPartitionedTables(CatalogType catalog_obj) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        Map<Table, MaterializedViewInfo> ret = new HashMap<Table, MaterializedViewInfo>();
        for (Table catalog_tbl : catalog_db.getTables()) {
            MaterializedViewInfo catalog_view = CatalogUtil.getVerticalPartition(catalog_tbl);
            if (catalog_view != null)
                ret.put(catalog_tbl, catalog_view);
        } // FOR
        return (ret);
    }

    /**
     * Return the VerticalPartition for the given Table
     * 
     * @param catalog_tbl
     * @return
     */
    public static MaterializedViewInfo getVerticalPartition(Table catalog_tbl) {
        for (MaterializedViewInfo catalog_view : catalog_tbl.getViews()) {
            if (catalog_view.getVerticalpartition()) {
                return catalog_view;
            }
        } // FOR
        return (null);
    }

    /**
     * Return the set of read-only Columns for the given table If
     * exclude_inserts is true, then only UPDATES will be counted against
     * columns
     * 
     * @param catalog_tbl
     * @param exclude_inserts
     * @return
     */
    public static Collection<Column> getReadOnlyColumns(Table catalog_tbl, boolean exclude_inserts) {
        assert (catalog_tbl != null);
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
     * 
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
                    if (to_column != null)
                        break;
                }
            } // FOR
            cache.FOREIGNKEY_PARENT.put(from_column, to_column);
        }
        return (to_column);
    }

    /**
     * Returns all the columns for this table that have a foreign key dependency
     * on another table
     * 
     * @param catalog_tbl
     * @return
     */
    public static Collection<Column> getForeignKeyDependents(Table catalog_tbl) {
        Set<Column> found = new ListOrderedSet<Column>();
        for (Column catalog_col : catalog_tbl.getColumns()) {
            assert (catalog_col != null);
            if (!catalog_col.getConstraints().isEmpty()) {
                // System.out.println(catalog_col + ": " +
                // CatalogUtil.getConstraints(catalog_col.getConstraints()));
                if (!CatalogUtil.findAll(CatalogUtil.getConstraints(catalog_col.getConstraints()), "type", ConstraintType.FOREIGN_KEY.getValue()).isEmpty()) {
                    found.add(catalog_col);
                }
            }
        } // FOR
        return (found);
    }
    
    public static String[] getChildTables(Database catalog_db, Table catalog_tbl){
        List<String> found= new ArrayList<String>(); 
        CatalogMap<Column> columns = catalog_tbl.getColumns();
        for (Table t : catalog_db.getTables()){
        	if(t!=catalog_tbl){
        		for(Column c: getForeignKeyDependents(t)){
        			Column parentColumn = CatalogUtil.getForeignKeyParent(c);
        			if(columns.containsKey(parentColumn.getName())){
        				found.add(t.getName());
        				break;
        			}
        		}
        	}
        }
        String [] tableNames = new String[found.size()];
        return found.toArray(tableNames);    	
    }

    /**
     * Return the real Column objects for the ColumnRefs
     * @param map
     * @return
     */
    public static Collection<Column> getColumns(Iterable<ColumnRef> map) {
        List<Column> ret = new ArrayList<Column>();
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
            Set<Table> tables = new ListOrderedSet<Table>();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                tables.addAll(CatalogUtil.getReferencedTables(catalog_stmt));
            } // FOR
            ret = Collections.unmodifiableSet(tables);
            cache.PROCEDURE_TABLES.put(catalog_proc, ret);
        }
        return (ret);
    }

    /**
     * Returns all the columns access/modified in all the Statements for this
     * Procedure
     * 
     * @param catalog_proc
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumns(Procedure catalog_proc) throws Exception {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_proc);
        Set<Column> ret = cache.PROCEDURE_COLUMNS.get(catalog_proc);
        if (ret == null) {
            Set<Column> columns = new ListOrderedSet<Column>();
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
     * 
     * @param catalog_tbl
     * @return
     * @throws Exception
     */
    public static Collection<Procedure> getReferencingProcedures(Table catalog_tbl) throws Exception {
        Set<Procedure> ret = new ListOrderedSet<Procedure>();
        Database catalog_db = CatalogUtil.getDatabase(catalog_tbl);
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getSystemproc())
                continue;
            if (CatalogUtil.getReferencedTables(catalog_proc).contains(catalog_tbl)) {
                ret.add(catalog_proc);
            }
        } // FOR
        return (ret);
    }

    /**
     * Returns all of the procedures that access/modify the given Column
     * 
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
                    if (catalog_proc.getSystemproc())
                        continue;
                    if (CatalogUtil.getReferencedColumns(catalog_proc).contains(col)) {
                        ret.add(catalog_proc);
                    }
                } // FOR
            } // FOR
        } else {
            for (Procedure catalog_proc : catalog_db.getProcedures()) {
                if (catalog_proc.getSystemproc())
                    continue;
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
     */
    public static Collection<Table> getReferencedTables(Statement catalog_stmt) {
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Collection<Table> ret = cache.STATEMENT_TABLES.get(catalog_stmt);
        if (ret == null) {
            Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
            AbstractPlanNode node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
            Collection<Table> tables = CatalogUtil.getReferencedTablesForTree(catalog_db, node);
            ret = Collections.unmodifiableCollection(tables);
            // cache.STATEMENT_TABLES.put(catalog_stmt, ret);
        }
        return (ret);
    }
    
    /**
     * Returns all the indexes access/modified in the given Statement's query
     * @param catalog_stmt
     */
    public static Collection<Index> getReferencedIndexes(Statement catalog_stmt) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        AbstractPlanNode node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
        Collection<Index> indexes = CatalogUtil.getReferencedIndexesForTree(catalog_db, node);
        return (Collections.unmodifiableCollection(indexes));
    }
    
    /**
     * Get all the tables referenced in this PlanFragment
     * @param catalog_fag
     */
    public static Collection<Table> getReferencedTables(PlanFragment catalog_frag) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_frag);
        AbstractPlanNode node = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
        return (CatalogUtil.getReferencedTablesForTree(catalog_db, node));
    }

    /**
     * Returns all the columns access/modified in the given Statement's query
     * This does not include the columns that are output by SELECT queries
     * 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumns(Statement catalog_stmt) {
        if (debug.val)
            LOG.debug("Extracting referenced columns from statement " + CatalogUtil.getDisplayName(catalog_stmt));

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Set<Column> ret = cache.STATEMENT_ALL_COLUMNS.get(catalog_stmt);
        if (ret == null) {
            final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
            ret = new ListOrderedSet<Column>();

            CatalogFieldComparator<Column> comparator = new CatalogFieldComparator<Column>("index");
            Set<Column> modified = new TreeSet<Column>(comparator);
            Set<Column> readOnly = new TreeSet<Column>(comparator);

            // 2010-07-14: Always use the AbstractPlanNodes from the PlanFragments 
            // to figure out what columns the query touches. It's more accurate 
            // because we will pick apart plan nodes and expression trees to figure things out
            AbstractPlanNode node = null;
            Set<Column> columns = null;
            try {
                node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
                columns = CatalogUtil.getReferencedColumnsForTree(catalog_db, node, ret, modified, readOnly);
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to get columns for " + catalog_stmt.fullName(), ex);
            }
            assert (columns != null) : "Failed to get columns for " + catalog_stmt.fullName();
            ret = Collections.unmodifiableSet(columns);
            cache.STATEMENT_ALL_COLUMNS.put(catalog_stmt, ret);
            cache.STATEMENT_MODIFIED_COLUMNS.put(catalog_stmt, Collections.unmodifiableSet(modified));
            cache.STATEMENT_READONLY_COLUMNS.put(catalog_stmt, Collections.unmodifiableSet(readOnly));
        }
        return (ret);
    }

    /**
     * Returns all the columns access/modified in the given Statement's query
     * 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Column> getModifiedColumns(Statement catalog_stmt) {
        if (debug.val)
            LOG.debug("Extracting modified columns from statement " + CatalogUtil.getDisplayName(catalog_stmt));

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Set<Column> ret = cache.STATEMENT_MODIFIED_COLUMNS.get(catalog_stmt);
        if (ret == null) {
            CatalogUtil.getReferencedColumns(catalog_stmt);
            ret = cache.STATEMENT_MODIFIED_COLUMNS.get(catalog_stmt);
        }
        return (ret);
    }

    /**
     * Returns all the columns that are not modified in the given Statement's
     * query
     * 
     * @param catalog_stmt
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReadOnlyColumns(Statement catalog_stmt) {
        if (debug.val)
            LOG.debug("Extracting read-only columns from statement " + CatalogUtil.getDisplayName(catalog_stmt));

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Collection<Column> ret = cache.STATEMENT_READONLY_COLUMNS.get(catalog_stmt);
        if (ret == null) {
            CatalogUtil.getReferencedColumns(catalog_stmt);
            ret = cache.STATEMENT_READONLY_COLUMNS.get(catalog_stmt);
        }
        return (ret);
    }

    /**
     * Return all the Columns used in ORDER BY clauses for the given Statement
     * 
     * @param catalog_stmt
     * @return
     */
    public static Collection<Column> getOrderByColumns(Statement catalog_stmt) {
        if (debug.val)
            LOG.debug("Extracting order-by columns from statement " + CatalogUtil.getDisplayName(catalog_stmt));

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Set<Column> ret = cache.STATEMENT_ORDERBY_COLUMNS.get(catalog_stmt);
        if (ret == null) {
            Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
            ret = new ListOrderedSet<Column>();
            try {
                AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
                assert (root != null);
                PlannerContext context = PlannerContext.singleton();
                for (OrderByPlanNode node : PlanNodeUtil.getPlanNodes(root, OrderByPlanNode.class)) {
                    for (Integer guid : node.getSortColumnGuids()) {
                        PlanColumn pcol = context.get(guid);
                        assert (pcol != null);
                        ret.addAll(ExpressionUtil.getReferencedColumns(catalog_db, pcol.getExpression()));
                    } // FOR
                } // FOR
            } catch (Throwable ex) {
                throw new RuntimeException("Failed to retrieve ORDER BY columns for " + catalog_stmt.fullName());
            }
            cache.STATEMENT_ORDERBY_COLUMNS.put(catalog_stmt, ret);
        }
        return (ret);
    }
    
    /**
     * Returns the set of Column catalog objects modified by the given
     * AbstractPlanNode. If you're looking for where we figure out what columns
     * a PlanNode touches that is of interest to us for figuring out how we will
     * partition things, then you've come to the right place.
     * 
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumnsForTree(final Database catalog_db, AbstractPlanNode node) throws Exception {
        return (getReferencedColumnsForTree(catalog_db, node, new ListOrderedSet<Column>(), null, null));
    }

    private static Set<Column> getReferencedColumnsForTree(final Database catalog_db, final AbstractPlanNode node, final Set<Column> columns, final Set<Column> modified, final Set<Column> readOnly)
            throws Exception {
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(final AbstractPlanNode node) {
                try {
                    CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, node, columns, modified, readOnly);
                } catch (Exception ex) {
                    LOG.fatal("Failed to extract columns from " + node, ex);
                    throw new RuntimeException(ex);
                }
                return;
            }
        }.traverse(node);
        return (columns);
    }

    /**
     * Return all of the columns referenced in the given AbstractPlanNode
     * Non-recursive.
     * 
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Column> getReferencedColumnsForPlanNode(final Database catalog_db, final AbstractPlanNode node) throws Exception {
        final Set<Column> ret = new ListOrderedSet<Column>();
        CatalogUtil.getReferencedColumnsForPlanNode(catalog_db, node, ret, null, null);
        return (ret);
    }

    private static void updateReferenceColumns(Collection<Column> cols, boolean is_readOnly, final Set<Column> ref_columns, final Set<Column> modified_cols, final Set<Column> readOnly) {
        for (Column col : cols) {
            updateReferenceColumns(col, is_readOnly, ref_columns, modified_cols, readOnly);
        } // FOR
    }

    private static void updateReferenceColumns(Column col, boolean isReadOnly, final Set<Column> allCols, final Set<Column> modifiedCols, final Set<Column> readOnlyCols) {
        allCols.add(col);
        if (modifiedCols != null) {
            assert (readOnlyCols != null);
            if (isReadOnly) {
                if (modifiedCols.contains(col) == false)
                    readOnlyCols.add(col);
            } else {
                readOnlyCols.remove(col);
                modifiedCols.add(col);
            }
        }
    }

    /**
     * Returns all the columns referenced for the given PlanNode If
     * modified_cols is not null, then it will contain the Columns that are
     * modified in the PlanNode
     * 
     * @param catalog_db
     * @param node
     * @param allCols
     * @param modifiedCols
     * @throws Exception
     */
    private static void getReferencedColumnsForPlanNode(final Database catalog_db,
                                                        final AbstractPlanNode node,
                                                        final Set<Column> allCols,
                                                        final Set<Column> modifiedCols,
                                                        final Set<Column> readOnlyCols) throws Exception {
        switch (node.getPlanNodeType()) {
        // ---------------------------------------------------
        // INSERT
        // ---------------------------------------------------
            case INSERT: {
                // All columns are accessed whenever we insert a new record
                InsertPlanNode ins_node = (InsertPlanNode) node;
                Table catalog_tbl = catalog_db.getTables().get(ins_node.getTargetTableName());
                assert (catalog_tbl != null) : "Missing table " + ins_node.getTargetTableName();
                updateReferenceColumns(catalog_tbl.getColumns(), false, allCols, modifiedCols, readOnlyCols);
                break;
            }
            // ---------------------------------------------------
            // UPDATE
            // ---------------------------------------------------
            case UPDATE: {
                // Need to make sure we get both the WHERE clause and the fields
                // that are updated
                // We need to get the list of columns from the ScanPlanNode
                // below us
                UpdatePlanNode up_node = (UpdatePlanNode) node;
                Table catalog_tbl = catalog_db.getTables().get(up_node.getTargetTableName());
                assert (catalog_tbl != null) : "Missing table " + up_node.getTargetTableName();

                AbstractScanPlanNode scan_node = CollectionUtil.first(PlanNodeUtil.getPlanNodes(up_node, AbstractScanPlanNode.class));
                assert (scan_node != null) : "Failed to find underlying scan node for " + up_node;

                updateReferenceColumns(PlanNodeUtil.getUpdatedColumnsForPlanNode(catalog_db, scan_node), false, allCols, modifiedCols, readOnlyCols);

                // XXX: Why is this necessary?
                if (scan_node.getInlinePlanNodeCount() > 0) {
                    ProjectionPlanNode proj_node = scan_node.getInlinePlanNode(PlanNodeType.PROJECTION);
                    assert (proj_node != null);

                    // This is a bit tricky. We have to go by the names of the
                    // output columns to find what
                    // column is meant to be updated
                    PlannerContext pcontext = PlannerContext.singleton();
                    for (Integer col_guid : proj_node.getOutputColumnGUIDs()) {
                        PlanColumn pc = pcontext.get(col_guid);
                        assert (pc != null);
                        if (pc.getExpression() instanceof TupleAddressExpression)
                            continue;

                        Column catalog_col = catalog_tbl.getColumns().get(pc.getDisplayName());
                        if (catalog_col == null) {
                            LOG.error("Invalid PlanNode:\n" + PlanNodeUtil.debug(up_node));
                        }
                        assert (catalog_col != null) : String.format("Missing %s.%s\n%s", catalog_tbl.getName(), pc.getDisplayName(), pc);

                        updateReferenceColumns(catalog_col, false, allCols, modifiedCols, readOnlyCols);
                    } // FOR
                }
                break;
            }
            // ---------------------------------------------------
            // DELETE
            // ---------------------------------------------------
            case DELETE:
                // I don't think we need anything here because all the
                // columns will get get picked up by the scans that feed into
                // the DELETE
                break;
            // ---------------------------------------------------
            // EVERYTHING ELSE
            // ---------------------------------------------------
            default: {
                Collection<AbstractExpression> node_exps = PlanNodeUtil.getExpressionsForPlanNode(node, PlanNodeType.PROJECTION, PlanNodeType.ORDERBY, PlanNodeType.AGGREGATE,
                        PlanNodeType.HASHAGGREGATE);
                for (AbstractExpression exp : node_exps) {
                    Collection<Column> columns = null;
                    try {
                        columns = ExpressionUtil.getReferencedColumns(catalog_db, exp);
                    } catch (Throwable ex) {
                        throw new RuntimeException("Failed to get referenced columns from " + exp + " extract from " + node, ex);
                    }
                    assert (columns != null);
                    updateReferenceColumns(columns, true, allCols, modifiedCols, readOnlyCols);
                } // FOR
            }
        } // SWITCH
    }

    /**
     * Return all of the tables referenced in the given AbstractPlanNode
     * Non-recursive.
     * 
     * @param catalog_db
     * @param node
     * @return
     * @throws Exception
     */
    public static Collection<Table> getReferencedTablesForPlanNode(final Database catalog_db,
                                                                   final AbstractPlanNode node) {
        final Set<Table> ret = new ListOrderedSet<Table>();
        try {
            CatalogUtil.getReferencedTablesForPlanNode(catalog_db, node, ret);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to get referenced tables for " + node, ex);
        }
        return (ret);
    }

    /**
     * Return all of tables referenced in the PlanNode tree, regardless if they
     * are modified or not
     * 
     * @param catalog_db
     * @param root
     * @return
     */
    public static Collection<Table> getReferencedTablesForTree(final Database catalog_db,
                                                               final AbstractPlanNode root) {
        final Set<Table> found = new ListOrderedSet<Table>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                CatalogUtil.getReferencedTablesForPlanNode(catalog_db, element, found);
                return;
            }
        }.traverse(root);
        return (found);
    }

    private static void getReferencedTablesForPlanNode(final Database catalog_db,
                                                       final AbstractPlanNode node,
                                                       final Collection<Table> found) {
        String table_name = null;
        // AbstractScanNode
        if (node instanceof AbstractScanPlanNode) {
            AbstractScanPlanNode cast_node = (AbstractScanPlanNode) node;
            table_name = cast_node.getTargetTableName();
            assert (table_name != null);
            assert (!table_name.isEmpty());
        }
        // NestLoopIndexNode
        else if (node instanceof NestLoopIndexPlanNode) {
            NestLoopIndexPlanNode cast_node = (NestLoopIndexPlanNode) node;
            assert (cast_node.getInlinePlanNodeCount() == 1);
            IndexScanPlanNode idx_node = cast_node.getInlinePlanNode(PlanNodeType.INDEXSCAN);
            table_name = idx_node.getTargetTableName();
        }
        // AbstractOperationPlanNode
        else if (node instanceof AbstractOperationPlanNode) {
            AbstractOperationPlanNode cast_node = (AbstractOperationPlanNode) node;
            table_name = cast_node.getTargetTableName();
            assert (table_name != null);
            assert (!table_name.isEmpty());
        }

        if (table_name != null) {
            Table catalog_tbl = catalog_db.getTables().get(table_name);
            assert (catalog_tbl != null) : String.format("Invalid table '%s' extracted from %s. Valid tables: %s", table_name, node, CatalogUtil.getDisplayNames(catalog_db.getTables()));
            found.add(catalog_tbl);
        }
    }
    
    /**
     * Return all of indexes referenced in the PlanNode tree, regardless if they
     * are modified or not
     * @param catalog_db
     * @param root
     * @return
     */
    public static Collection<Index> getReferencedIndexesForTree(final Database catalog_db,
                                                                final AbstractPlanNode root) {
        final Set<Index> found = new ListOrderedSet<Index>();
        new PlanNodeTreeWalker(true) {
            @Override
            protected void callback(AbstractPlanNode element) {
                // IndexScanNode
                if (element instanceof IndexScanPlanNode) {
                    IndexScanPlanNode cast_node = (IndexScanPlanNode)element;
                    String table_name = cast_node.getTargetTableName();
                    assert(table_name != null);
                    assert(table_name.isEmpty() == false);
                    
                    String index_name = cast_node.getTargetIndexName();
                    assert(index_name != null);
                    assert(index_name.isEmpty() == false);
                
                    Table catalog_tbl = catalog_db.getTables().get(table_name);
                    assert(catalog_tbl != null) :
                        String.format("Invalid table '%s' extracted from %s. Valid tables: %s",
                                      table_name, element, CatalogUtil.getDisplayNames(catalog_db.getTables()));
                    
                    Index catalog_idx = catalog_tbl.getIndexes().get(index_name);
                    assert(catalog_idx != null) :
                        String.format("Invalid index '%s.%s' extracted from %s. Valid indexes: %s",
                                table_name, index_name, element,
                                CatalogUtil.getDisplayNames(catalog_tbl.getIndexes()));
                    found.add(catalog_idx);
                }
                return;
            }
        }.traverse(root);
        return (found);
    }
    
    /**
     * Extract a ColumnSet for the given Statement catalog object for all Tables
     * If convert_params is set to true, then we will convert any StmtParameters
     * that are mapped to ProcParameter directly into the ProcParameter object.
     * 
     * @param catalog_stmt
     * @param convert_params
     * @return
     * @throws Exception
     */
    public static PredicatePairs extractStatementPredicates(final Statement catalog_stmt, final boolean convert_params) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        Table tables[] = catalog_db.getTables().values();
        return (CatalogUtil.extractStatementPredicates(catalog_stmt, convert_params, tables));
    }

    /**
     * Returns true if the given query is an equality predicate (with all the columns)
     * on a unique index. This essentially means that the query will only return
     * one row at runtime.
     * <B>WARNING:</B> This is wildly inaccurate and should not used by to make runtime decisions.
     * @param catalog_stmt
     * @return
     */
    public static boolean isEqualityIndexScan(Statement catalog_stmt) {
        Collection<Table> tables = getReferencedTables(catalog_stmt);
        PredicatePairs cset = extractStatementPredicates(catalog_stmt, false, tables.toArray(new Table[0]));
        
        Collection<Index> indexes = getReferencedIndexes(catalog_stmt);
        
        for (CatalogPair cp : cset) {
            if (cp.getComparisonExp() != ExpressionType.COMPARE_EQUAL) {
                return (false);
            }
            
            CatalogType ctypes[] = { cp.getFirst(), cp.getSecond() };
            for (int i = 0; i < ctypes.length; i++) {
                if (ctypes[i] instanceof Column) {
                    Column target_col = (Column)ctypes[i];
                    Table target_tbl = target_col.getParent();

                    // Find what index it's using
                    // This is a rough approximation...
                    Index target_idx = null;
                    for (Index idx : indexes) {
                        if (idx.getParent().equals(target_tbl)) {
                            for (Column col : CatalogUtil.getColumns(idx.getColumns())) {
                                if (col.equals(target_col)) {
                                    target_idx = idx;
                                    break;
                                }
                            } // FOR
                        }
                    } // FOR
                    if (target_idx == null) {
                        return (false);
                    }
                }
            } // FOR

        } // FOR
        return (true);
    }
    
    /**
     * Extract a ColumnSet for the given Statement catalog object that only
     * consists of the Columns that are referenced in the list of tables. If
     * convert_params is set to true, then we will convert any StmtParameters
     * that are mapped to ProcParameter directly into the ProcParameter object.
     * 
     * @param catalog_stmt
     * @param convert_params
     * @param catalog_tables
     * @return
     * @throws Exception
     */
    public static PredicatePairs extractStatementPredicates(final Statement catalog_stmt,
                                                            final boolean convert_params,
                                                            final Table... catalog_tables) {
        final Database catalog_db = (Database) catalog_stmt.getParent().getParent();
        final Set<Table> tables = new HashSet<Table>();
        final Collection<String> table_keys = new HashSet<String>();
        for (Table table : catalog_tables) {
            // For some reason we get a null table when we use itemsArray up above
            if (table == null)
                continue;
            assert (table != null) : "Null table object? " + Arrays.toString(catalog_tables);
            tables.add(table);
            table_keys.add(CatalogKey.createKey(table));
        } // FOR

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Pair<String, Collection<String>> key = Pair.of(CatalogKey.createKey(catalog_stmt), table_keys);
        PredicatePairs cset = cache.EXTRACTED_PREDICATES.get(key);
        if (cset == null) {
            cset = new PredicatePairs();
            AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);

            try {
                // WHERE Clause
                if (catalog_stmt.getExptree() != null && !catalog_stmt.getExptree().isEmpty()) {
                    AbstractExpression root_exp = ExpressionUtil.deserializeExpression(catalog_db, catalog_stmt.getExptree());
                    CatalogUtil.extractExpressionPredicates(catalog_stmt,
                                                           catalog_db,
                                                           cset,
                                                           root_exp,
                                                           convert_params,
                                                           tables);
                }
                // INSERT
                if (catalog_stmt.getQuerytype() == QueryType.INSERT.getValue()) {
                    CatalogUtil.extractInsertPredicates(catalog_stmt,
                                                       cset,
                                                       root_node,
                                                       convert_params,
                                                       catalog_tables);
                    // UPDATE
                    // XXX: Should we be doing this?
                } else if (catalog_stmt.getQuerytype() == QueryType.UPDATE.getValue()) {
                    CatalogUtil.extractUpdatePredicates(catalog_stmt,
                                                       catalog_db,
                                                       cset,
                                                       root_node,
                                                       convert_params,
                                                       tables);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to extract ColumnSet for " + catalog_stmt, ex);
            }

            cache.EXTRACTED_PREDICATES.put(key, cset);
        }
        return (cset);
    }

    /**
     * Extract ColumnSet for the SET portion of an UPDATE statement
     * 
     * @param catalog_stmt
     * @param catalog_tables
     * @return
     * @throws Exception
     */
    public static PredicatePairs extractUpdateColumnSet(final Statement catalog_stmt,
                                                   final boolean convert_params,
                                                   final Table... catalog_tables) throws Exception {
        assert (catalog_stmt.getQuerytype() == QueryType.UPDATE.getValue());
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);

        final Set<Table> tables = new HashSet<Table>();
        final Collection<String> table_keys = new HashSet<String>();
        for (Table table : catalog_tables) {
            tables.add(table);
            table_keys.add(CatalogKey.createKey(table));
        } // FOR

        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Pair<String, Collection<String>> key = Pair.of(CatalogKey.createKey(catalog_stmt), table_keys);
        PredicatePairs cset = cache.EXTRACTED_PREDICATES.get(key);
        if (cset == null) {
            cset = new PredicatePairs();
            AbstractPlanNode root_node = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, true);
            CatalogUtil.extractUpdatePredicates(catalog_stmt, catalog_db, cset, root_node, convert_params, tables);
            cache.EXTRACTED_PREDICATES.put(key, cset);
        }
        return (cset);
    }

    /**
     * @param stats_catalog_db
     * @param last_cset
     * @param root_exp
     * @param catalog_tables
     */
    public static PredicatePairs extractFragmentPredicates(final PlanFragment catalog_frag,
                                                     final boolean convert_params,
                                                     final Collection<Table> catalog_tables) throws Exception {
        final Statement catalog_stmt = (Statement) catalog_frag.getParent();
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        // if (catalog_frag.guid == 279) LOG.setLevel(Level.DEBUG);

        // We need to be clever about what we're doing here
        // We always have to examine the fragment (rather than just the entire
        // Statement), because
        // we don't know whehter they want the multi-sited version or not
        final Collection<String> table_keys = CatalogKey.createKeys(catalog_tables);

        if (debug.val)
            LOG.debug("Extracting column set for fragment #" + catalog_frag.getName() + ": " + catalog_tables);
        final CatalogUtil.Cache cache = CatalogUtil.getCatalogCache(catalog_stmt);
        Pair<String, Collection<String>> key = Pair.of(CatalogKey.createKey(catalog_frag), table_keys);
        PredicatePairs cset = cache.EXTRACTED_PREDICATES.get(key);
        if (cset == null) {
            AbstractPlanNode root_node = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frag);
            // LOG.debug("PlanFragment Node:\n" +
            // PlanNodeUtil.debug(root_node));
            cset = new PredicatePairs();
            CatalogUtil.extractPlanNodePredicates(catalog_stmt, catalog_db, cset, root_node, convert_params, catalog_tables);
            cache.EXTRACTED_PREDICATES.put(key, cset);
        }

        return (cset);
    }

    /**
     * @param catalog_stmt
     * @param catalogContext
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractInsertPredicates(final Statement catalog_stmt,
                                               final PredicatePairs cset,
                                               final AbstractPlanNode root_node,
                                               final boolean convert_params,
                                               final Table... catalog_tables) throws Exception {
        assert (catalog_stmt.getQuerytype() == QueryType.INSERT.getValue());
        final Database catalog_db = CatalogUtil.getDatabase(catalog_stmt);
        final List<List<CatalogType>> materialize_elements = new ArrayList<List<CatalogType>>();

        // Find the MaterializePlanNode that feeds into the Insert
        // This will have the list of columns that will be used to insert into
        // the table
        new PlanNodeTreeWalker() {
            @Override
            protected void callback(final AbstractPlanNode node) {
                // We should find the Materialize node before the Insert
                if (node instanceof MaterializePlanNode) {
                    for (Integer column_guid : node.getOutputColumnGUIDs()) {
                        PlanColumn column = PlannerContext.singleton().get(column_guid);
                        assert (column != null);
                        AbstractExpression exp = column.getExpression();

                        // Now extract the CatalogType objects that are being
                        // referenced by this materialization column
                        final List<CatalogType> catalog_refs = new ArrayList<CatalogType>();
                        new ExpressionTreeWalker() {
                            @Override
                            protected void callback(AbstractExpression exp) {
                                if (!(exp instanceof AbstractValueExpression))
                                    return;
                                CatalogType element = null;
                                switch (exp.getExpressionType()) {
                                    case VALUE_PARAMETER: {
                                        int param_idx = ((ParameterValueExpression) exp).getParameterId();
                                        element = catalog_stmt.getParameters().get(param_idx);
                                        if (element == null) {
                                            LOG.warn("ERROR: Unable to find Parameter object in catalog [" + ((ParameterValueExpression) exp).getParameterId() + "]");
                                            this.stop();
                                        }
                                        // We want to use the ProcParameter instead of the StmtParameter
                                        // It's not an error if the StmtParameter is not mapped to a
                                        // ProcParameter
                                        if (convert_params && ((StmtParameter) element).getProcparameter() != null) {
                                            LOG.debug(element + "(" + element + ") --> ProcParameter[" + element.getField("procparameter") + "]");
                                            element = ((StmtParameter) element).getProcparameter();
                                        }
                                        break;
                                    }
                                    case VALUE_TUPLE_ADDRESS:
                                    case VALUE_TUPLE: {
                                        // This shouldn't happen, but it is nice
                                        // to be told if it does...
                                        LOG.warn("Unexpected " + exp.getClass().getSimpleName() + " node when examining " + node.getClass().getSimpleName() + " for " + catalog_stmt);
                                        break;
                                    }
                                    default: {
                                        // Do nothing...
                                    }
                                } // SWITCH
                                if (element != null) {
                                    catalog_refs.add(element);
                                    LOG.debug(node + ": " + catalog_refs);
                                }
                                return;
                            }
                        }.traverse(exp);
                        materialize_elements.add(catalog_refs);
                    } // FOR

                    // InsertPlanNode
                } else if (node instanceof InsertPlanNode) {
                    InsertPlanNode insert_node = (InsertPlanNode) node;
                    Table catalog_tbl = catalog_db.getTables().get(insert_node.getTargetTableName());

                    // We only support when the Materialize node is inserting
                    // data into all columns
                    if (materialize_elements.size() != catalog_tbl.getColumns().size()) {
                        String msg = String.format("%s has %d columns but the MaterializePlanNode has %d output columns", catalog_tbl, catalog_tbl.getColumns().size(), materialize_elements.size());
                        LOG.fatal(PlanNodeUtil.debug(node));
                        throw new RuntimeException(msg);
                    }

                    // Loop through each column position and add an entry in the
                    // ColumnSet for
                    // each catalog item that was used in the
                    // MaterializePlanNode
                    // For example, if the INSERT clause for a column FOO was
                    // "PARAM1 + PARAM2", then
                    // the column set will have separate entries for FOO->PARAM1
                    // and FOO->PARAM2
                    LOG.debug("Materialize Elements: " + materialize_elements);
                    for (int ctr = 0, cnt = materialize_elements.size(); ctr < cnt; ctr++) {
                        Column catalog_col = catalog_tbl.getColumns().get(ctr);
                        assert (catalog_col != null);
                        for (CatalogType catalog_item : materialize_elements.get(ctr)) {
                            cset.add(catalog_col, catalog_item, ExpressionType.COMPARE_EQUAL, catalog_stmt);
                            LOG.debug(String.format("[%02d] Adding Entry %s => %s", ctr, catalog_col, catalog_item));
                        } // FOR
                    } // FOR
                }
                return;
            }
        }.traverse(root_node);
        return;
    }

    /**
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractPlanNodePredicates(final Statement catalog_stmt,
                                                 final Database catalog_db,
                                                 final PredicatePairs cset,
                                                 final AbstractPlanNode root_node,
                                                 final boolean convert_params,
                                                 final Collection<Table> tables) throws Exception {
        // Walk through the tree and figure out how the tables are being
        // referenced
        new PlanNodeTreeWalker() {
            {
                this.setAllowRevisit(true);
            }

            protected void populate_children(PlanNodeTreeWalker.Children<AbstractPlanNode> children, AbstractPlanNode node) {
                super.populate_children(children, node);
                List<AbstractPlanNode> to_add = new ArrayList<AbstractPlanNode>();
                for (AbstractPlanNode child : children.getBefore()) {
                    to_add.addAll(child.getInlinePlanNodes().values());
                } // FOR
                children.addBefore(to_add);

                to_add.clear();
                for (AbstractPlanNode child : children.getAfter()) {
                    to_add.addAll(child.getInlinePlanNodes().values());
                } // FOR
                children.addAfter(to_add);

                if (debug.val) {
                    LOG.debug(children);
                    LOG.debug("-------------------------");
                }
            };

            @Override
            protected void callback(final AbstractPlanNode node) {
                try {
                    if (debug.val)
                        LOG.debug("Examining child node " + node);
                    this._callback(node);
                } catch (Exception ex) {
                    ex.printStackTrace();
                    throw new RuntimeException(ex);
                }
            }

            protected void _callback(final AbstractPlanNode node) throws Exception {
                ListOrderedSet<AbstractExpression> exps = new ListOrderedSet<AbstractExpression>();

                // IndexScanPlanNode
                if (node instanceof IndexScanPlanNode) {
                    IndexScanPlanNode cast_node = (IndexScanPlanNode) node;
                    Table catalog_tbl = catalog_db.getTables().get(cast_node.getTargetTableName());
                    assert (catalog_tbl != null);
                    Index catalog_idx = catalog_tbl.getIndexes().get(cast_node.getTargetIndexName());
                    assert (catalog_idx != null);

                    // Search Key Expressions
                    List<ColumnRef> index_cols = CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index");
                    for (int i = 0, cnt = cast_node.getSearchKeyExpressions().size(); i < cnt; i++) {
                        AbstractExpression index_exp = cast_node.getSearchKeyExpressions().get(i);
                        Column catalog_col = index_cols.get(i).getColumn();
                        if (debug.val)
                            LOG.debug("[" + i + "] " + catalog_col);
                        exps.add(CatalogUtil.createTempExpression(catalog_col, index_exp));
                        if (debug.val)
                            LOG.debug("Added temp index search key expression:\n" + ExpressionUtil.debug(exps.get(exps.size() - 1)));
                    } // FOR

                    // End Expression
                    if (cast_node.getEndExpression() != null) {
                        exps.add(cast_node.getEndExpression());
                        if (debug.val)
                            LOG.debug("Added scan end expression:\n" + ExpressionUtil.debug(exps.get(exps.size() - 1)));
                    }

                    // Post-Scan Expression
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (debug.val)
                            LOG.debug("Added post-scan predicate:\n" + ExpressionUtil.debug(exps.get(exps.size() - 1)));
                    }

                    // SeqScanPlanNode
                } else if (node instanceof SeqScanPlanNode) {
                    SeqScanPlanNode cast_node = (SeqScanPlanNode) node;
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (debug.val)
                            LOG.debug("Adding scan node predicate:\n" + ExpressionUtil.debug(exps.get(exps.size() - 1)));
                    }

                    // Materialize
                } else if (node instanceof MaterializePlanNode) {
                    // Assume that if we're here, then they want the mappings
                    // from columns to StmtParameters
                    assert (tables.size() == 1);
                    Table catalog_tbl = CollectionUtil.first(tables);
                    for (int ctr = 0, cnt = node.getOutputColumnGUIDs().size(); ctr < cnt; ctr++) {
                        int column_guid = node.getOutputColumnGUIDs().get(ctr);
                        PlanColumn column = PlannerContext.singleton().get(column_guid);
                        assert (column != null);

                        Column catalog_col = catalog_tbl.getColumns().get(column.getDisplayName());
                        assert (catalog_col != null) : "Invalid column name '" + column.getDisplayName() + "' for " + catalog_tbl;

                        AbstractExpression exp = column.getExpression();
                        if (exp instanceof ParameterValueExpression) {
                            StmtParameter catalog_param = catalog_stmt.getParameters().get(((ParameterValueExpression) exp).getParameterId());
                            cset.add(catalog_col, catalog_param, ExpressionType.COMPARE_EQUAL, catalog_stmt);
                        } else if (exp instanceof AbstractValueExpression) {
                            if (debug.val)
                                LOG.debug("Ignoring AbstractExpressionType type: " + exp);
                        } else {
                            throw new Exception("Unexpected AbstractExpression type: " + exp);
                        }

                    } // FOR
                    // Join Nodes
                } else if (node instanceof AbstractJoinPlanNode) {
                    AbstractJoinPlanNode cast_node = (AbstractJoinPlanNode) node;
                    if (cast_node.getPredicate() != null) {
                        exps.add(cast_node.getPredicate());
                        if (debug.val)
                            LOG.debug("Added join node predicate: " + ExpressionUtil.debug(exps.get(exps.size() - 1)));
                    }
                }

                if (debug.val)
                    LOG.debug("Extracting expressions information from " + node + " for tables " + tables);
                for (AbstractExpression exp : exps) {
                    if (exp == null)
                        continue;
                    CatalogUtil.extractExpressionPredicates(catalog_stmt, catalog_db, cset, exp, convert_params, tables);
                } // FOR
                return;
            }
        }.traverse(root_node);
        return;
    }

    /**
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_exp
     * @param tables
     * @throws Exception
     */
    public static void extractExpressionPredicates(final Statement catalog_stmt,
                                                   final Database catalog_db,
                                                   final PredicatePairs cset,
                                                   final AbstractExpression root_exp,
                                                   final boolean convert_params,
                                                   final Collection<Table> tables) throws Exception {
        if (debug.val)
            LOG.debug(catalog_stmt + "\n" + ExpressionUtil.debug(root_exp));

        new ExpressionTreeWalker() {
            {
                this.setAllowRevisit(true);
            }

            boolean on_leftside = true;
            ExpressionType compare_exp = null;
            final Set<CatalogType> left = new HashSet<CatalogType>();
            final Set<CatalogType> right = new HashSet<CatalogType>();
            final Set<Table> used_tables = new HashSet<Table>();

            private void populate() {
                if (debug.val) {
                    LOG.debug("POPULATE!");
                    LOG.debug("LEFT:  " + this.left);
                    LOG.debug("RIGHT: " + this.right);
                    LOG.debug("USED:  " + this.used_tables);
                }
                //
                // Both sides can't be empty and our extract items must cover
                // all the tables that we are looking for. That is, if we were
                // asked
                // to lookup information on two tables (i.e., a JOIN), then we
                // need
                // to have extract information from our tree that uses those two
                // tables
                //
                if (!this.left.isEmpty() && !this.right.isEmpty()) { // &&
                                                                     // (this.used_tables.size()
                                                                     // ==
                                                                     // tables.size()))
                                                                     // {
                    for (CatalogType left_element : this.left) {
                        for (CatalogType right_element : this.right) {
                            if (debug.val)
                                LOG.debug("Added entry: [" + left_element + " " + this.compare_exp + " " + right_element + "]");
                            cset.add(left_element, right_element, this.compare_exp, catalog_stmt);
                            if (debug.val)
                                LOG.debug("ColumnSet:\n" + cset.debug());
                        } // FOR
                    } // FOR
                }
                this.left.clear();
                this.right.clear();
                this.used_tables.clear();
                this.on_leftside = true;
                this.compare_exp = null;
                return;
            }

            @Override
            protected void callback(AbstractExpression exp) {
                if (debug.val)
                    LOG.debug("CALLBACK(counter=" + this.getCounter() + ") " + exp.getClass().getSimpleName());
                // if (exp instanceof ComparisonExpression) {
                // LOG.debug("\n" + ExpressionUtil.debug(exp));
                // }

                // ComparisionExpression
                // We need to switch from the left to the right element set.
                // TODO: This assumes that they aren't doing something funky
                // like "(col1 AND col2) AND col3"
                if (exp instanceof ComparisonExpression) {
                    if (!this.on_leftside) {
                        LOG.error("ERROR: Invalid expression tree format : Unexpected ComparisonExpression");
                        this.stop();
                        return;
                    }
                    this.on_leftside = false;
                    this.compare_exp = exp.getExpressionType();

                    // Special Case: IN
                    if (exp instanceof InComparisonExpression) {
                        InComparisonExpression in_exp = (InComparisonExpression) exp;
                        for (AbstractExpression value_exp : in_exp.getValues()) {
                            this.processValueExpression(value_exp);
                        } // FOR
                        this.populate();
                    }
                }
                // When we hit a conjunction, we need to make a cross
                // product of the left and
                // right attribute sets
                else if (exp instanceof ConjunctionExpression) {
                    this.populate();
                }
                // Values: NULL, Tuple, Constants, Parameters
                else if (exp instanceof AbstractValueExpression) {
                    this.processValueExpression(exp);
                }
                return;
            }

            /**
             * @param exp
             */
            private void processValueExpression(AbstractExpression exp) {
                CatalogType element = null;
                switch (exp.getExpressionType()) {
                    case VALUE_TUPLE: {
                        String table_name = ((TupleValueExpression) exp).getTableName();
                        String column_name = ((TupleValueExpression) exp).getColumnName();
                        if (debug.val)
                            LOG.debug("VALUE TUPLE: " + table_name + "." + column_name);
                        Table catalog_tbl = catalog_db.getTables().get(table_name);

                        // Always use because we don't know whether the next
                        // table will be one of the ones that we are looking for
                        if (tables.contains(catalog_tbl)) {
                            if (debug.val)
                                LOG.debug("FOUND VALUE_TUPLE: " + table_name);
                            element = catalog_tbl.getColumns().get(column_name);
                            this.used_tables.add(catalog_tbl);
                        }
                        break;
                    }
                    case VALUE_TUPLE_ADDRESS: {
                        //
                        // ????
                        //
                        // String table_name =
                        // ((TupleAddressExpression)exp).getTableName();
                        // String column_name =
                        // ((TupleAddressExpression)exp).getColumnName();
                        // element = CatalogUtil.getColumn(catalog_db,
                        // table_name, column_name);
                        break;
                    }
                    case VALUE_CONSTANT: {
                        element = new ConstantValue();
                        ((ConstantValue) element).setIs_null(false);
                        ((ConstantValue) element).setType(((ConstantValueExpression) exp).getValueType().getValue());
                        ((ConstantValue) element).setValue(((ConstantValueExpression) exp).getValue());
                        break;
                    }
                    case VALUE_PARAMETER: {
                        int param_idx = ((ParameterValueExpression) exp).getParameterId();
                        element = catalog_stmt.getParameters().get(param_idx);
                        if (element == null) {
                            LOG.warn("ERROR: Unable to find Parameter object in " + catalog_stmt.fullName() + " [" + ((ParameterValueExpression) exp).getParameterId() + "]");
                            this.stop();
                        }
                        // We want to use the ProcParameter instead of the StmtParameter
                        // It's not an error if the StmtParameter is not mapped
                        // to a ProcParameter
                        if (convert_params && ((StmtParameter) element).getProcparameter() != null) {
                            element = ((StmtParameter) element).getProcparameter();
                        }

                        break;
                    }
                    default:
                        // Do nothing...
                } // SWITCH
                if (element != null) {
                    if (this.on_leftside)
                        this.left.add(element);
                    else
                        this.right.add(element);
                    if (debug.val) {
                        LOG.debug("LEFT: " + this.left);
                        LOG.debug("RIGHT: " + this.right);
                    }
                }
            }

            @Override
            protected void callback_last(AbstractExpression element) {
                this.populate();
            }
        }.traverse(root_exp);
        return;
    }

    /**
     * @param catalog_stmt
     * @param catalog_db
     * @param cset
     * @param root_node
     * @param tables
     * @throws Exception
     */
    public static void extractUpdatePredicates(final Statement catalog_stmt,
                                               final Database catalog_db,
                                               final PredicatePairs cset,
                                               final AbstractPlanNode root_node,
                                               final boolean convert_params,
                                               final Collection<Table> tables) throws Exception {
        // Grab the columns that the plannode is going to update from the
        // children feeding into us.
        Collection<UpdatePlanNode> update_nodes = PlanNodeUtil.getPlanNodes(root_node, UpdatePlanNode.class);
        for (UpdatePlanNode update_node : update_nodes) {
            Table catalog_tbl = catalog_db.getTables().get(update_node.getTargetTableName());
            // Grab all the scan nodes that are feeding into us
            Collection<AbstractScanPlanNode> scan_nodes = PlanNodeUtil.getPlanNodes(root_node, AbstractScanPlanNode.class);
            assert (!scan_nodes.isEmpty());
            for (AbstractScanPlanNode scan_node : scan_nodes) {
                List<PlanColumn> output_cols = new ArrayList<PlanColumn>();
                List<AbstractExpression> output_exps = new ArrayList<AbstractExpression>();
                if (scan_node.getOutputColumnGUIDs().isEmpty()) {
                    if (scan_node.getInlinePlanNode(PlanNodeType.PROJECTION) != null) {
                        ProjectionPlanNode proj_node = (ProjectionPlanNode) scan_node.getInlinePlanNode(PlanNodeType.PROJECTION);
                        for (int guid : proj_node.getOutputColumnGUIDs()) {
                            PlanColumn column = PlannerContext.singleton().get(guid);
                            assert (column != null);
                            output_cols.add(column);
                            output_exps.add(column.getExpression());
                        } // FOR
                    }
                } else {
                    for (int guid : scan_node.getOutputColumnGUIDs()) {
                        PlanColumn column = PlannerContext.singleton().get(guid);
                        assert (column != null);
                        output_cols.add(column);
                        output_exps.add(column.getExpression());
                    } // FOR
                }

                for (int i = 0, cnt = output_cols.size(); i < cnt; i++) {
                    PlanColumn column = output_cols.get(i);
                    AbstractExpression exp = output_exps.get(i);
                    // Skip TupleAddressExpression
                    if (!(exp instanceof TupleAddressExpression)) {
                        //
                        // Make a temporary expression where COL = Expression
                        // Har har har! I'm so clever!
                        //
                        String column_name = (column.originColumnName() != null ? column.originColumnName() : column.getDisplayName());
                        Column catalog_col = catalog_tbl.getColumns().get(column_name);
                        if (catalog_col == null)
                            System.err.println(catalog_tbl + ": " + CatalogUtil.debug(catalog_tbl.getColumns()));
                        assert (catalog_col != null) : "Missing column '" + catalog_tbl.getName() + "." + column_name + "'";
                        AbstractExpression root_exp = CatalogUtil.createTempExpression(catalog_col, exp);
                        // System.out.println(ExpressionUtil.debug(root_exp));
                        CatalogUtil.extractExpressionPredicates(catalog_stmt, catalog_db, cset, root_exp, convert_params, tables);
                    }
                } // FOR
            }
            // System.out.println(PlanNodeUtil.debug(root_node));
        }
    }

    /**
     * Create a temporary column expression that can be used with
     * extractExpressionColumnSet
     * 
     * @param catalog_col
     * @param exp
     * @return
     */
    private static AbstractExpression createTempExpression(Column catalog_col, AbstractExpression exp) {
        Table catalog_tbl = (Table) catalog_col.getParent();

        TupleValueExpression tuple_exp = new TupleValueExpression();
        tuple_exp.setTableName(catalog_tbl.getName());
        tuple_exp.setColumnIndex(catalog_col.getIndex());
        tuple_exp.setColumnAlias(catalog_col.getName());
        tuple_exp.setColumnName(catalog_col.getName());

        return (new ComparisonExpression(ExpressionType.COMPARE_EQUAL, tuple_exp, exp));
    }

    /**
     * Return a PlanFragment for a given id. This is slow and should only be
     * used for debugging purposes
     * 
     * @param catalog_obj
     * @param id
     * @return
     */
    public static PlanFragment getPlanFragment(CatalogType catalog_obj, int id) {
        Database catalog_db = CatalogUtil.getDatabase(catalog_obj);
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                for (PlanFragment catalog_frag : catalog_stmt.getFragments())
                    if (catalog_frag.getId() == id)
                        return (catalog_frag);
                for (PlanFragment catalog_frag : catalog_stmt.getMs_fragments())
                    if (catalog_frag.getId() == id)
                        return (catalog_frag);
            } // FOR (stmt)
        } // FOR (proc)
        return (null);
    }

    // ------------------------------------------------------------
    // UTILITY METHODS
    // ------------------------------------------------------------

    /**
     * Return an ordered list of the parents for each of the catalog items
     */
    public static Collection<CatalogType> getParents(Iterable<? extends CatalogType> catalog_items) {
        Collection<CatalogType> parents = new ArrayList<CatalogType>();
        for (CatalogType item : catalog_items) {
            if (item != null) {
                parents.add(item.getParent());
            } else {
                parents.add(null);
            }
        } // FOR
        return (parents);
    }

    /**
     * @param <T>
     * @param <U>
     * @param items
     * @param field
     * @param value
     * @return
     */
    public static <T extends CatalogType, U> Set<T> findAll(Iterable<T> items, String field, U value) {
        Set<T> found = new ListOrderedSet<T>();
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
     * @param <T>
     * @param <U>
     * @param items
     * @param field
     * @param value
     * @return
     */
    public static <T extends CatalogType, U> T findOne(Iterable<T> items, String field, U value) {
        return (CollectionUtil.first(CatalogUtil.findAll(items, field, value)));
    }

    /**
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
     * @param item
     * @return
     */
    public static String getDisplayName(CatalogType item) {
        return (CatalogUtil.getDisplayName(item, false));
    }

    /**
     * Return the list of display names for a collection of CatalogType
     * 
     * @param items
     * @return
     */
    public static Collection<String> getDisplayNames(Iterable<? extends CatalogType> items) {
        List<String> ret = new ArrayList<String>();
        for (CatalogType i : items) {
            ret.add(i != null ? CatalogUtil.getDisplayName(i, false) : null);
        }
        return (ret);
    }
    
    /**
     * Return the list of display names for a collection of CatalogType
     * 
     * @param items
     * @return
     */
    public static Collection<String> getDisplayNames(CatalogType...items) {
        List<String> ret = new ArrayList<String>();
        for (CatalogType i : items) {
            ret.add(i != null ? CatalogUtil.getDisplayName(i, false) : null);
        }
        return (ret);
    }
    
    /**
     * Return a mapping from the CatalogType handle to their display name
     * @param items
     * @return
     */
    public static Map<CatalogType, String> getDisplayNameMapping(CatalogType...items) {
        Map<CatalogType, String> ret = new HashMap<CatalogType, String>();
        for (CatalogType i : items) {
            if (i == null) continue;
            ret.put(i, CatalogUtil.getDisplayName(i, false));
        } // FOR
        return (ret);
    }
    
    /**
     * Return a mapping from the CatalogType handle to their display name
     * @param items
     * @return
     */
    public static Map<CatalogType, String> getDisplayNameMapping(Iterable<? extends CatalogType> items) {
        Map<CatalogType, String> ret = new HashMap<CatalogType, String>();
        for (CatalogType i : items) {
            if (i == null) continue;
            ret.put(i, CatalogUtil.getDisplayName(i, false));
        } // FOR
        return (ret);
    }

    /**
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
            }
            // StmtParameter
            // Format: <Procedure>.<Statement>.<Item>
            else if (item instanceof StmtParameter) {
                ret = String.format("%s{%s.%s.#%02d}",
                                    item.getClass().getSimpleName(),
                                    item.getParent().getParent().getName(),
                                    item.getParent().getName(),
                                    ((StmtParameter)item).getIndex());
            }    
            // ProcParameter
            // Format: <Parent>.<Item>
            else if (item instanceof ProcParameter) {
                ret = String.format("%s{%s.#%02d}",
                                    item.getClass().getSimpleName(),
                                    item.getParent().getName(),
                                    ((ProcParameter)item).getIndex());
            }
            // PlanFragment
            // Format: <Procedure>.<Statement>.[Fragment #XYZ]
            else if (item instanceof PlanFragment) {
                ret = String.format("%s.%s.[Fragment #%s]", item.getParent().getParent().getName(), item.getParent().getName(), item.getName());
            }
            // ConstantValue
            // Format: ConstantValue{XYZ}
            else if (item instanceof ConstantValue) {
                ret = String.format("%s{%s}", item.getClass().getSimpleName(), ((ConstantValue) item).getValue());
            }
            // Everything Else
            // Format: <OptionalClassName>.<Item>
            else {
                ret = String.format("%s%s", (include_class ? item.getClass().getSimpleName() + ":" : ""), item.getName());
            }
            return (ret);
        }
        return (null);
    }

    /**
     * @param catalog_stmt
     * @return
     */
    public static String debugJSON(Statement catalog_stmt) {
        String jsonString = Encoder.hexDecodeToString(catalog_stmt.getFullplan());
        String line = "\n----------------------------------------\n";
        String ret = "FULL PLAN ORIG STRING:\n" + jsonString + line;

        for (PlanFragment catalog_frgmt : catalog_stmt.getFragments()) {
            jsonString = Encoder.hexDecodeToString(catalog_frgmt.getPlannodetree());
            try {
                JSONObject jsonObject = new JSONObject(jsonString);
                ret += "FRAGMENT " + catalog_frgmt.getName() + "\n" + jsonObject.toString(2) + line;
            } catch (Exception ex) {
                ex.printStackTrace();
                throw new RuntimeException(ex);
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
        Map<String, Object> m = new ListOrderedMap<String, Object>();
        for (String field : CollectionUtils.union(catalog_item.getFields(), catalog_item.getChildFields())) {
            String val_str = null;
            if (catalog_item.getChildFields().contains(field)) {
                val_str = CatalogUtil.debug(catalog_item.getChildren(field));
            } else {
                Object val = catalog_item.getField(field);
                if (val != null) {
                    val_str = val.toString();
                } else {
                    val_str = "null";
                }
            }
            m.put(field, val_str);
        } // FOR
        return (catalog_item.fullName() + "\n" + StringUtil.formatMaps(m));
    }

    public static String debug(Collection<? extends CatalogType> items) {
        if (items == null)
            return (null);
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