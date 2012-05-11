/***************************************************************************
 *   Copyright (C) 2011 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown;

import java.io.File;
import java.io.IOException;
import java.lang.Thread.UncaughtExceptionHandler;
import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltType;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;
import org.voltdb.catalog.*;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.benchmark.auctionmark.AuctionMarkProjectBuilder;
import edu.brown.benchmark.markov.MarkovProjectBuilder;
import edu.brown.benchmark.seats.SEATSProjectBuilder;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tpce.TPCEProjectBuilder;
import edu.brown.benchmark.wikipedia.WikipediaProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.logging.LoggerUtil;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;
import edu.brown.hstore.conf.HStoreConf;

/**
 * Base class that provides a lot of the common functionality that our HStore test cases need
 * @author pavlo
 */
public abstract class BaseTestCase extends TestCase implements UncaughtExceptionHandler {
    private static final Logger LOG = Logger.getLogger(BaseTestCase.class);

    protected static final boolean ENABLE_JAR_REUSE;
    
    static {
        // log4j Hack
        LoggerUtil.setupLogging();
        
        // Jar Caching!
        boolean reuse = false;
        if (System.getenv("ENABLE_JAR_REUSE") != null) {
            reuse = Boolean.valueOf(System.getenv("ENABLE_JAR_REUSE"));
            if (reuse) LOG.debug("ENABLE_JAR_REUSE = " + reuse);
        }
        ENABLE_JAR_REUSE = reuse;
        
        // HStoreConf Hack
        HStoreConf.init(null, null);
        HStoreConf.singleton().site.cpu_affinity = false;
        
        // Force everything to be single-threaded
        ThreadUtil.setMaxGlobalThreads(2);
    }

    protected ProjectType last_type;
    
    /**
     * There is always a static catalog that gets created for each project type
     * This is so that for each test case invocation we don't have to recompile the catalog every time
     */
    protected static Catalog catalog;
    private static final Map<ProjectType, Catalog> project_catalogs = new HashMap<ProjectType, Catalog>();
    
    protected static Database catalog_db;
    private static final Map<ProjectType, Database> project_databases = new HashMap<ProjectType, Database>();

    protected static PartitionEstimator p_estimator;
    private static final Map<ProjectType, PartitionEstimator> project_p_estimators = new HashMap<ProjectType, PartitionEstimator>();

    private static final Map<ProjectType, AbstractProjectBuilder> project_builders = new HashMap<ProjectType, AbstractProjectBuilder>();
    
    private static Boolean is_first = null;
    
    /**
     * Setup the test case for the given project type
     * By default we don't include foreign keys in the catalog (I forget why we did this)
     * @param type
     * @throws Exception
     */
    protected void setUp(ProjectType type) throws Exception {
        this.setUp(type, false);
    }
    
    /**
     * Setup the test case for the given project type
     * @param type
     * @param fkeys - if true, then 
     * @throws Exception
     */
    protected void setUp(ProjectType type, boolean fkeys) throws Exception {
        this.setUp(type, fkeys, true);
    }
    
    protected void setUp(AbstractProjectBuilder projectBuilder) throws Exception {
        this.setUp(projectBuilder, false);
    }
    
    protected void setUp(AbstractProjectBuilder projectBuilder, boolean force) throws Exception {
        super.setUp();
        is_first = (is_first == null ? true : false);
        this.last_type = ProjectType.TEST;
        if (force == false) {
            catalog = project_catalogs.get(this.last_type);
            catalog_db = project_databases.get(this.last_type);
            p_estimator = project_p_estimators.get(this.last_type);
        }
        if (catalog == null || force) {
            String catalogJar = new File(projectBuilder.getJarName(true)).getAbsolutePath();
            try {
                boolean status = projectBuilder.compile(catalogJar);
                assert (status);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to create " + projectBuilder.getProjectName() + " catalog [" + catalogJar + "]", ex);
            }
    
            catalog = new Catalog();
            try {
                // read in the catalog
                String serializedCatalog = JarReader.readFileFromJarfile(catalogJar, CatalogUtil.CATALOG_FILENAME);
                // create the catalog (that will be passed to the ClientInterface
                catalog.execute(serializedCatalog);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load " + projectBuilder.getProjectName() + " catalog [" + catalogJar + "]", ex);
            }
            
            this.init(this.last_type, catalog);
        }
    }
    
    /**
     * Main setUp method for test cases. Given the ProjectType we will populate the static catalog field members 
     * The full_catalog flag is a hack to work around OutofMemory issues with TPC-E
     * @param type
     * @param fkeys
     * @param full_catalog
     * @throws Exception
     */
    protected void setUp(ProjectType type, boolean fkeys, boolean full_catalog) throws Exception {
        super.setUp();
        is_first = (is_first == null ? true : false);
        this.last_type = type;
        catalog = project_catalogs.get(type);
        catalog_db = project_databases.get(type);
        p_estimator = project_p_estimators.get(type);
        if (catalog == null) {
            AbstractProjectBuilder projectBuilder = AbstractProjectBuilder.getProjectBuilder(type);
            if (ENABLE_JAR_REUSE) {
                File jar_path = projectBuilder.getJarPath(true);
                if (jar_path.exists()) {
                    LOG.debug("LOAD CACHE JAR: " + jar_path.getAbsolutePath());
                    catalog = CatalogUtil.loadCatalogFromJar(jar_path.getAbsolutePath());
                } else {
                    LOG.debug("MISSING JAR: " + jar_path.getAbsolutePath());
                }
            }
            if (catalog == null) {
                switch (type) {
//                    case TPCC:
//                        catalog = TPCCProjectBuilder.getTPCCSchemaCatalog(true);
//                        // Update the ProcParameter mapping used in the catalogs
////                        ParametersUtil.populateCatalog(CatalogUtil.getDatabase(catalog), ParametersUtil.getParameterMapping(type));
//                        break;
                    case TPCE:
                        catalog = projectBuilder.createCatalog(fkeys, full_catalog);
                        break;
                    case TPCC:
                    case TM1:
                    case SEATS:
                    case AUCTIONMARK:
                    case MARKOV:
                    case LOCALITY:
                    case MAPREDUCE:
                    case WIKIPEDIA:
                        catalog = projectBuilder.getFullCatalog(fkeys);
                        if (LOG.isDebugEnabled()) 
                            LOG.debug(type + " Catalog JAR: " + projectBuilder.getJarPath(true).getAbsolutePath());
                        break;
                    default:
                        assert(false) : "Invalid project type - " + type;
                } // SWITCH
            }
            //if (type == ProjectType.TPCC) ParametersUtil.populateCatalog(CatalogUtil.getDatabase(catalog), ParametersUtil.getParameterMapping(type));
            this.init(type, catalog);
        }
    }
    
    /**
     * Store the catalog for this ProjectType and generate the supporting classes
     * @param type
     * @param catalog
     */
    private void init(ProjectType type, Catalog catalog) {
        assertNotNull(catalog);
        project_catalogs.put(type, catalog);
        
        catalog_db = CatalogUtil.getDatabase(catalog);
        assertNotNull(catalog_db);
        project_databases.put(type, catalog_db);
        
        p_estimator = new PartitionEstimator(catalog_db);
        assertNotNull(p_estimator);
        project_p_estimators.put(type, p_estimator);
    }

    public static AbstractProjectBuilder getProjectBuilder(ProjectType type) {
        AbstractProjectBuilder projectBuilder = project_builders.get(type);
        if (projectBuilder == null) {
            switch (type) {
                case TPCC:
                    projectBuilder = new TPCCProjectBuilder();
                    break;
                case TPCE:
                    projectBuilder = new TPCEProjectBuilder();
                    break;
                case TM1:
                    projectBuilder = new TM1ProjectBuilder();
                    break;
                case SEATS:
                    projectBuilder = new SEATSProjectBuilder();
                    break;
                case AUCTIONMARK:
                    projectBuilder = new AuctionMarkProjectBuilder();
                    break;
                case MARKOV:
                    projectBuilder = new MarkovProjectBuilder();
                    break;
                case WIKIPEDIA:
                    projectBuilder = new WikipediaProjectBuilder();
                    break;
                default:
                    assert(false) : "Invalid project type - " + type;
            } // SWITCH
            project_builders.put(type, projectBuilder);
        }
        assert(projectBuilder != null);
        return (projectBuilder);
    }
    
    public static File getCatalogJarPath(ProjectType type) {
        return (getProjectBuilder(type).getJarPath(true));
    }
    public static File getDDLPath(ProjectType type) {
        return (new File(getProjectBuilder(type).getDDLURL(true).getFile()));
    }
    /**
     * Returns true if this is the first time setup() has been called
     * Useful for updating the catalog
     * @return
     */
    public static Boolean isFirstSetup() {
        return (is_first);
    }
    
    /**
     * Returns true if we have access to the Volt lib in our local system
     * @return
     */
    public boolean hasVoltLib() throws Exception {
        File obj_dir = FileUtil.findDirectory("obj");
        
        // Figure out whether we are on a machine that has the native lib
        // we can use right now
        if (obj_dir != null) {
            File so_path = new File(obj_dir.getAbsolutePath() + "/release/nativelibs/libvoltdb.so");
            if (so_path.exists()) {
                System.load(so_path.getAbsolutePath());
                return (true);
            }
        }
        return (false);
    }
    
    protected void applyCatalogCorrelations(ProjectType type) throws Exception {
        // We need the correlations file in order to make sure the parameters 
        // get mapped properly
        File correlations_path = this.getParameterMappingsFile(type);
        if (correlations_path != null) {
            ParameterMappingsSet correlations = new ParameterMappingsSet();
            correlations.load(correlations_path.getAbsolutePath(), catalog_db);
            ParametersUtil.applyParameterMappings(catalog_db, correlations);
        }
    }
    
    // --------------------------------------------------------------------------------------
    // CONVENIENCE METHODS
    // --------------------------------------------------------------------------------------
    
    protected Cluster getCluster() {
        assertNotNull(catalog);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        assert(catalog_clus != null) : "Failed to retriever cluster object from catalog";
        return (catalog_clus);
    }
    
    protected Site getSite(int site_id) {
        assertNotNull(catalog);
        Cluster catalog_clus = this.getCluster();
        Site catalog_site = catalog_clus.getSites().get("id", site_id);
        assert(catalog_site != null) : "Failed to retrieve Site #" + site_id + " from catalog";
        return (catalog_site);
    }
    
    protected Table getTable(Database catalog_db, String table_name) {
        assertNotNull(catalog_db);
        Table catalog_tbl = catalog_db.getTables().get(table_name);
        assert(catalog_tbl != null) : "Failed to retrieve '" + table_name + "' table from catalog"; 
        return (catalog_tbl);
    }
    protected Table getTable(String table_name) {
        return getTable(catalog_db, table_name);
    }

    protected Column getColumn(Database catalog_db, Table catalog_tbl, String col_name) {
        assertNotNull(catalog_db);
        assertNotNull(catalog_tbl);
        Column catalog_col = catalog_tbl.getColumns().getIgnoreCase(col_name);
        assert(catalog_col != null) : "Failed to retrieve Column '" + col_name + "' from Table '" + catalog_tbl.getName() + "'";
        return (catalog_col);
    }
    protected Column getColumn(Table catalog_tbl, String col_name) {
        return (getColumn(catalog_db, catalog_tbl, col_name));
    }
    protected Column getColumn(Database catalog_db, String table_name, String col_name) {
        return (getColumn(catalog_db, this.getTable(catalog_db, table_name), col_name));
    }
    protected Column getColumn(String table_name, String col_name) {
        return (getColumn(catalog_db, this.getTable(table_name), col_name));
    }
    protected Column getColumn(Table catalog_tbl, int col_idx) {
        int num_columns = catalog_tbl.getColumns().size();
        if (col_idx < 0) col_idx = num_columns + col_idx; // Python!
        assert(col_idx >= 0 && col_idx < num_columns) : "Invalid column index for " + catalog_tbl + ": " + col_idx;
        Column catalog_col = catalog_tbl.getColumns().get(col_idx); 
        assert(catalog_col != null) : "Failed to retrieve Column at '" + col_idx + "' from Table '" + catalog_tbl.getName() + "'";
        return (catalog_col);
    }

    protected Procedure getProcedure(Database catalog_db, String proc_name) {
        assertNotNull(catalog_db);
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : "Failed to retrieve '" + proc_name + "' Procedure from catalog"; 
        return (catalog_proc);
    }
    protected Procedure getProcedure(String proc_name) {
        return getProcedure(catalog_db, proc_name);
    }
    protected Procedure getProcedure(Database catalog_db, Class<? extends VoltProcedure> proc_class) {
        return getProcedure(catalog_db, proc_class.getSimpleName());
    }
    protected Procedure getProcedure(Class<? extends VoltProcedure> proc_class) {
        return getProcedure(catalog_db, proc_class.getSimpleName());
    }
    
    protected ProcParameter getProcParameter(Database catalog_db, Procedure catalog_proc, int idx) {
        assertNotNull(catalog_db);
        assert(idx >= 0) : "Invalid ProcParameter index for " + catalog_proc + ": " + idx;
        assert(idx < catalog_proc.getParameters().size()) : "Invalid ProcParameter index for " + catalog_proc + ": " + idx;
        ProcParameter catalog_param = catalog_proc.getParameters().get(idx);
        assertNotNull("Null ProcParameter index for " + catalog_proc + ": " + idx, catalog_param);
        return (catalog_param);
    }
    protected ProcParameter getProcParameter(Procedure catalog_proc, int idx) {
        return getProcParameter(catalog_db, catalog_proc, idx);
    }
    protected ProcParameter getProcParameter(Class<? extends VoltProcedure> proc_class, int idx) {
        return getProcParameter(catalog_db, this.getProcedure(proc_class), idx);
    }

    protected Statement getStatement(Database catalog_db, Procedure catalog_proc, String stmt_name) {
        assertNotNull(catalog_db);
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().get(stmt_name);
        assert(catalog_stmt != null) : "Failed to retrieve Statement '" + stmt_name + "' from Procedure '" + catalog_proc.getName() + "'";
        return (catalog_stmt);
    }
    protected Statement getStatement(Procedure catalog_proc, String stmt_name) {
        return getStatement(catalog_db, catalog_proc, stmt_name);
    }
    
    /**
     * Add fake partitions to the loaded catalog
     * Assuming that there is one partition per site
     * @param num_partitions
     */
    protected void addPartitions(int num_partitions) throws Exception {
        // HACK! If we already have this many partitions in the catalog, then we won't recreate it
        // This fixes problems where we need to reference the same catalog objects in multiple test cases
        if (CatalogUtil.getNumberOfPartitions(catalog_db) != num_partitions) {
            ClusterConfiguration cc = new ClusterConfiguration();
            for (Integer i = 0; i < num_partitions; i++) {
                cc.addPartition("localhost", 0, i);
                // System.err.println("[" + i + "] " + Arrays.toString(triplets.lastElement()));
            } // FOR
            catalog = FixCatalog.addHostInfo(catalog, cc);
            this.init(this.last_type, catalog);
            
        }
        Cluster cluster = CatalogUtil.getCluster(catalog_db);
        assertEquals(num_partitions, cluster.getNum_partitions());
        assertEquals(num_partitions, CatalogUtil.getNumberOfPartitions(cluster));
    }
    
    protected void initializeCluster(int num_hosts, int num_sites, int num_partitions) throws Exception {
        // HACK! If we already have this many partitions in the catalog, then we won't recreate it
        // This fixes problems where we need to reference the same catalog objects in multiple test cases
        if (CatalogUtil.getNumberOfHosts(catalog_db) != num_hosts ||
            CatalogUtil.getNumberOfSites(catalog_db) != (num_hosts * num_sites) ||
            CatalogUtil.getNumberOfPartitions(catalog_db) != (num_hosts * num_sites * num_partitions)) {
            catalog = FixCatalog.addHostInfo(catalog, "localhost", num_hosts, num_sites, num_partitions);
            this.init(this.last_type, catalog);
        }
        Cluster cluster = CatalogUtil.getCluster(catalog_db);
        assertEquals(num_hosts, CatalogUtil.getNumberOfHosts(catalog_db));
        assertEquals((num_hosts * num_sites), CatalogUtil.getNumberOfSites(catalog_db));
        assertEquals((num_hosts * num_sites * num_partitions), CatalogUtil.getNumberOfPartitions(cluster));
        assertEquals((num_hosts * num_sites * num_partitions), cluster.getNum_partitions());
    }
    
    // --------------------------------------------------------------------------------------
    // FILE LOADING METHODS
    // --------------------------------------------------------------------------------------
    
    /**
     * Find a trace file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getWorkloadFile(ProjectType type) throws IOException {
        String suffix = "";
        switch (type) {
            case TPCC:
                suffix = ".100p-1";
                break;
            default:
                suffix = "-1";
                break;
        } // SWITCH
        return (this.getWorkloadFile(type, suffix));
    }
    
    public File getWorkloadFile(ProjectType type, String suffix) throws IOException {
        return (this.getProjectFile(new File(".").getCanonicalFile(), type, "workloads", suffix+".trace"));
    }
    
    /**
     * Find a stats cache file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getStatsFile(ProjectType type) throws IOException {
        return (this.getProjectFile(new File(".").getCanonicalFile(), type, "stats", ".stats"));
    }
    
    /**
     * Find a parameter correlations file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getParameterMappingsFile(ProjectType type) throws IOException {
        return (this.getProjectFile(new File(".").getCanonicalFile(), type, "mappings", ".mappings"));
    }
    
//    /**
//     * Find a Markov file for a given project type
//     * @param current
//     * @param type
//     * @return
//     * @throws IOException
//     */
//    public File getMarkovFile(ProjectType type) throws IOException {
//        return (this.getProjectFile(new File(".").getCanonicalFile(), type, "markovs", ".markovs"));
//    }
    
    /**
     * 
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    private File getProjectFile(File current, ProjectType type, String target_dir, String target_ext) throws IOException {
        boolean has_svn = false;
        for (File file : current.listFiles()) {
            if (file.getCanonicalPath().endsWith("files") && file.isDirectory()) {
                // Look for either a .<target_ext> or a .<target_ext>.gz file
                String file_name = type.name().toLowerCase() + target_ext;
                for (int i = 0; i < 2; i++) {
                    if (i > 0) file_name += ".gz";
                    File target_file = new File(file + File.separator + target_dir + File.separator + file_name);
                    if (target_file.exists() && target_file.isFile()) {
                        return (target_file);
                    }
                } // FOR
                assert(false) : "Unable to find '" + file_name + "' for '" + type + "' in directory '" + file + "'";
            // Make sure that we don't go to far down...
            } else if (file.getCanonicalPath().endsWith("/.svn")) {
                has_svn = true;
            }
        } // FOR
        assert(has_svn) : "Unable to find files directory [last_dir=" + current.getAbsolutePath() + "]";  
        File next = new File(current.getCanonicalPath() + File.separator + "..");
        return (this.getProjectFile(next, type, target_dir, target_ext));
    }
 
    /**
     * Generate an array of random input parameters for a given Statement
     * @param catalog_stmt
     * @return
     */
    protected Object[] makeRandomStatementParameters(Statement catalog_stmt) {
        Object params[] = new Object[catalog_stmt.getParameters().size()];
        for (StmtParameter catalog_param : catalog_stmt.getParameters()) {
            VoltType vtype = VoltType.get(catalog_param.getJavatype());
            params[catalog_param.getIndex()] = VoltTypeUtil.getRandomValue(vtype);
            LOG.debug(catalog_param.fullName() + " -> " + params[catalog_param.getIndex()] + " / " + vtype);
        } // FOR
        return (params);
    }
    
    
    
    @Override
    public void uncaughtException(Thread t, Throwable e) {
        e.printStackTrace();
        fail(e.getMessage()); // XXX: I don't think this gets picked up
    }
}