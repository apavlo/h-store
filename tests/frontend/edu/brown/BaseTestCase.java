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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.TestCase;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltSystemProcedure;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.client.Client;
import org.voltdb.client.ClientFactory;
import org.voltdb.utils.JarReader;
import org.voltdb.utils.VoltTypeUtil;

import edu.brown.benchmark.AbstractProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.ClusterConfiguration;
import edu.brown.catalog.FixCatalog;
import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.logging.LoggerUtil;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.ParametersUtil;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;
import edu.brown.utils.ThreadUtil;

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

    private ProjectType last_type;
    
    /**
     * There is always a static catalog that gets created for each project type
     * This is so that for each test case invocation we don't have to recompile the catalog every time
     */
    protected static CatalogContext catalogContext;
    @Deprecated
    protected static Catalog catalog;
    @Deprecated
    protected static Database catalog_db;
    private static final Map<ProjectType, CatalogContext> project_catalogs = new HashMap<ProjectType, CatalogContext>();
    private static final Set<ProjectType> needs_reset = new HashSet<ProjectType>(); 
    
    protected static PartitionEstimator p_estimator;
    private static final Map<ProjectType, PartitionEstimator> project_p_estimators = new HashMap<ProjectType, PartitionEstimator>();

    private final static AtomicBoolean is_first = new AtomicBoolean(true);
    
    /**
     * Setup the test case for the given project type
     * By default we don't include foreign keys in the catalog (I forget why we did this)
     * @param type
     * @throws Exception
     */
    protected void setUp(ProjectType type) throws Exception {
        this.setUp(type, false, true);
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
        this.last_type = ProjectType.TEST;
        if (force == false) {
            catalogContext = project_catalogs.get(this.last_type);
        }
        if (catalogContext == null || force) {
            String catalogJar = new File(projectBuilder.getJarName(true)).getAbsolutePath();
            try {
                boolean status = projectBuilder.compile(catalogJar);
                assert(status) : "Failed to compile " + catalogJar;
            } catch (Exception ex) {
                throw new RuntimeException("Failed to create " + projectBuilder.getProjectName() + " catalog [" + catalogJar + "]", ex);
            }
    
            Catalog c = new Catalog();
            try {
                // read in the catalog
                String serializedCatalog = JarReader.readFileFromJarfile(catalogJar, CatalogUtil.CATALOG_FILENAME);
                // create the catalog (that will be passed to the ClientInterface
                c.execute(serializedCatalog);
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load " + projectBuilder.getProjectName() + " catalog [" + catalogJar + "]", ex);
            }
            
            CatalogContext cc = new CatalogContext(c, catalogJar);
            this.init(this.last_type, cc);
        }
        
        catalog = catalogContext.catalog;
        catalog_db = catalogContext.database;
        p_estimator = project_p_estimators.get(this.last_type);
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
        this.last_type = type;
        catalogContext = project_catalogs.get(type);
        
        if (catalogContext == null) {
            CatalogContext cc = null;
            AbstractProjectBuilder projectBuilder = getProjectBuilder(type);
            
            if (ENABLE_JAR_REUSE) {
                File jar_path = projectBuilder.getJarPath(true);
                if (needs_reset.contains(type)) {
                    jar_path.delete();
                    needs_reset.remove(type);
                }
                if (jar_path.exists()) {
                    LOG.debug("LOAD CACHE JAR: " + jar_path.getAbsolutePath());
                    cc = CatalogUtil.loadCatalogContextFromJar(jar_path);
                } else {
                    LOG.debug("MISSING JAR: " + jar_path.getAbsolutePath());
                }
            }
            if (cc == null) {
                File jarPath = projectBuilder.getJarPath(true);
                Catalog c = null;
                switch (type) {
                    case TPCE:
                        c = projectBuilder.createCatalog(fkeys, full_catalog);
                        break;
                    default:
                        c = projectBuilder.getFullCatalog(fkeys);
                        if (LOG.isDebugEnabled()) 
                            LOG.debug(type + " Catalog JAR: " + jarPath.getAbsolutePath());
                        break;
                } // SWITCH
                assert(c != null);
                cc = new CatalogContext(c, jarPath);
            }
            assert(cc != null) : "Unexpected null catalog for " + type;
            //if (type == ProjectType.TPCC) ParametersUtil.populateCatalog(CatalogUtil.getDatabase(catalog), ParametersUtil.getParameterMapping(type));
            this.init(type, cc);
        }
        catalog = catalogContext.catalog;
        catalog_db = catalogContext.database;
        p_estimator = project_p_estimators.get(type);
    }
    
    /**
     * Reset all cached objects for this project type, including
     * removing any jars that many already exist.
     * @param type
     */
    protected void reset(ProjectType type) {
        p_estimator = null;
        project_catalogs.remove(type);
        project_p_estimators.remove(type);
        needs_reset.add(type);
    }
    
    /**
     * Store the catalog for this ProjectType and generate the supporting classes
     * @param type
     * @param catalog
     */
    private void init(ProjectType type, CatalogContext cc) {
        assertNotNull(cc);
        project_catalogs.put(type, cc);
        catalogContext = cc;
        catalog = catalogContext.catalog;
        catalog_db = catalogContext.database;
        
        p_estimator = new PartitionEstimator(catalogContext);
        assertNotNull(p_estimator);
        project_p_estimators.put(type, p_estimator);
    }

    public static AbstractProjectBuilder getProjectBuilder(ProjectType type) {
        AbstractProjectBuilder projectBuilder = AbstractProjectBuilder.getProjectBuilder(type);
        // 2012-10-11: We have to disable any replicated secondary indexes
        // because it will screw up a bunch of tests that we already have setup
        projectBuilder.enableReplicatedSecondaryIndexes(false);
        projectBuilder.removeReplicatedSecondaryIndexes();
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
     * Returns true if this is the first time setup() has been called.
     * This method will always return true the first time it is invoked.
     * All subsequent calls will return false.
     * Useful for updating the catalog.
     */
    protected final static boolean isFirstSetup() {
        return (is_first.compareAndSet(true, false));
    }
    
    /**
     * Returns true if we have access to the Volt lib in our local system
     * @return
     */
    protected final boolean hasVoltLib() throws Exception {
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
    
    protected final void applyParameterMappings(ProjectType type) throws Exception {
        // We need the correlations file in order to make sure the parameters 
        // get mapped properly
        File correlations_path = this.getParameterMappingsFile(type);
        if (correlations_path != null) {
            ParameterMappingsSet correlations = new ParameterMappingsSet();
            correlations.load(correlations_path, catalog_db);
            ParametersUtil.applyParameterMappings(catalog_db, correlations);
        }
    }
    
    // --------------------------------------------------------------------------------------
    // CONVENIENCE METHODS
    // --------------------------------------------------------------------------------------
    
    protected final CatalogContext getCatalogContext() {
        assertNotNull(catalogContext);
        return (catalogContext);
    }
    
    protected final Catalog getCatalog() {
        assertNotNull(catalogContext);
        return (catalogContext.catalog);
    }
    
    protected final Database getDatabase() {
        assertNotNull(catalogContext);
        return (catalogContext.database);
    }
    
    protected final Cluster getCluster() {
        assertNotNull(catalogContext);
        return (catalogContext.cluster);
    }
    
    protected final Site getSite(int site_id) {
        assertNotNull(catalogContext);
        Site catalog_site = catalogContext.getSiteById(site_id);
        assert(catalog_site != null) : "Failed to retrieve Site #" + site_id + " from catalog";
        return (catalog_site);
    }
    
    protected final Table getTable(Database catalog_db, String table_name) {
        assertNotNull(catalog_db);
        Table catalog_tbl = catalog_db.getTables().getIgnoreCase(table_name);
        assert(catalog_tbl != null) : "Failed to retrieve '" + table_name + "' table from catalog"; 
        return (catalog_tbl);
    }
    protected final Table getTable(String table_name) {
        return getTable(catalog_db, table_name);
    }

    protected final Column getColumn(Database catalog_db, Table catalog_tbl, String col_name) {
        assertNotNull(catalog_db);
        assertNotNull(catalog_tbl);
        Column catalog_col = catalog_tbl.getColumns().getIgnoreCase(col_name);
        assert(catalog_col != null) : "Failed to retrieve Column '" + col_name + "' from Table '" + catalog_tbl.getName() + "'";
        return (catalog_col);
    }
    protected final Column getColumn(Table catalog_tbl, String col_name) {
        return (getColumn(catalog_db, catalog_tbl, col_name));
    }
    protected final Column getColumn(Database catalog_db, String table_name, String col_name) {
        return (getColumn(catalog_db, this.getTable(catalog_db, table_name), col_name));
    }
    protected final Column getColumn(String table_name, String col_name) {
        return (getColumn(catalog_db, this.getTable(table_name), col_name));
    }
    protected final Column getColumn(Table catalog_tbl, int col_idx) {
        int num_columns = catalog_tbl.getColumns().size();
        if (col_idx < 0) col_idx = num_columns + col_idx; // Python!
        assert(col_idx >= 0 && col_idx < num_columns) : "Invalid column index for " + catalog_tbl + ": " + col_idx;
        Column catalog_col = catalog_tbl.getColumns().get(col_idx); 
        assert(catalog_col != null) : "Failed to retrieve Column at '" + col_idx + "' from Table '" + catalog_tbl.getName() + "'";
        return (catalog_col);
    }

    protected final Procedure getProcedure(Database catalog_db, String proc_name) {
        assertNotNull(catalog_db);
        Procedure catalog_proc = catalog_db.getProcedures().getIgnoreCase(proc_name);
        assert(catalog_proc != null) : "Failed to retrieve '" + proc_name + "' Procedure from catalog"; 
        return (catalog_proc);
    }
    protected final Procedure getProcedure(String proc_name) {
        return getProcedure(catalog_db, proc_name);
    }
    @SuppressWarnings("unchecked")
    protected final Procedure getProcedure(Database catalog_db, Class<? extends VoltProcedure> proc_class) {
        String procName;
        if (ClassUtil.getSuperClasses(proc_class).contains(VoltSystemProcedure.class)) {
            procName = VoltSystemProcedure.procCallName((Class<? extends VoltSystemProcedure>)proc_class);
        } else {
            procName = proc_class.getSimpleName();
        }
        return getProcedure(catalog_db, procName);
    }
    protected final Procedure getProcedure(Class<? extends VoltProcedure> proc_class) {
        return getProcedure(catalog_db, proc_class);        
    }
    
    protected final ProcParameter getProcParameter(Database catalog_db, Procedure catalog_proc, int idx) {
        assertNotNull(catalog_db);
        assert(idx >= 0) : "Invalid ProcParameter index for " + catalog_proc + ": " + idx;
        assert(idx < catalog_proc.getParameters().size()) : "Invalid ProcParameter index for " + catalog_proc + ": " + idx;
        ProcParameter catalog_param = catalog_proc.getParameters().get(idx);
        assertNotNull("Null ProcParameter index for " + catalog_proc + ": " + idx, catalog_param);
        return (catalog_param);
    }
    protected final ProcParameter getProcParameter(Procedure catalog_proc, int idx) {
        return getProcParameter(catalog_db, catalog_proc, idx);
    }
    protected final ProcParameter getProcParameter(Class<? extends VoltProcedure> proc_class, int idx) {
        return getProcParameter(catalog_db, this.getProcedure(proc_class), idx);
    }

    protected final Statement getStatement(Database catalog_db, Procedure catalog_proc, String stmt_name) {
        assertNotNull(catalog_db);
        assertNotNull(catalog_proc);
        Statement catalog_stmt = catalog_proc.getStatements().getIgnoreCase(stmt_name);
        assert(catalog_stmt != null) : "Failed to retrieve Statement '" + stmt_name + "' from Procedure '" + catalog_proc.getName() + "'";
        return (catalog_stmt);
    }
    protected final Statement getStatement(Procedure catalog_proc, String stmt_name) {
        return getStatement(catalog_db, catalog_proc, stmt_name);
    }
    protected final Statement getStatement(Class<? extends VoltProcedure> proc_class, String stmt_name) {
        Procedure catalog_proc = getProcedure(proc_class);
        return getStatement(catalog_db, catalog_proc, stmt_name);
    }
    
    /**
     * Add fake partitions to the loaded catalog
     * Assuming that there is one partition per site
     * @param num_partitions
     */
    protected final void addPartitions(int num_partitions) throws Exception {
        // HACK! If we already have this many partitions in the catalog, then we won't recreate it
        // This fixes problems where we need to reference the same catalog objects in multiple test cases
        if (catalogContext.numberOfPartitions != num_partitions) {
            ClusterConfiguration cc = new ClusterConfiguration();
            for (Integer i = 0; i < num_partitions; i++) {
                cc.addPartition("localhost", 0, i);
                // System.err.println("[" + i + "] " + Arrays.toString(triplets.lastElement()));
            } // FOR
            Catalog c = FixCatalog.cloneCatalog(catalog, cc);
            this.init(this.last_type, new CatalogContext(c, catalogContext.jarPath));
            
        }
        Cluster cluster = CatalogUtil.getCluster(catalog_db);
        assertEquals(num_partitions, cluster.getNum_partitions());
        assertEquals(num_partitions, catalogContext.numberOfPartitions);
    }
    
    /**
     * Update the catalog to include new hosts/sites/partitions
     * @param num_hosts
     * @param num_sites
     * @param num_partitions
     * @throws Exception
     */
    protected final void initializeCatalog(int num_hosts, int num_sites, int num_partitions) throws Exception {
        // HACK! If we already have this many partitions in the catalog, then we won't recreate it
        // This fixes problems where we need to reference the same catalog objects in multiple test cases
        if (catalogContext.numberOfHosts != num_hosts ||
            catalogContext.numberOfSites != (num_hosts * num_sites) ||
            catalogContext.numberOfPartitions != (num_hosts * num_sites * num_partitions)) {
            
            // HACK
            String hostFormat = (num_hosts == 1 ? "localhost" : "host%02d"); 
            Catalog c = FixCatalog.cloneCatalog(catalog, hostFormat, num_hosts, num_sites, num_partitions);
            CatalogContext cc = new CatalogContext(c, catalogContext.jarPath);
            this.init(this.last_type, cc);
        }
        Cluster cluster = catalogContext.cluster;
        assertEquals(num_hosts, catalogContext.numberOfHosts);
        assertEquals((num_hosts * num_sites), catalogContext.numberOfSites);
        assertEquals((num_hosts * num_sites * num_partitions), catalogContext.numberOfPartitions);
        assertEquals((num_hosts * num_sites * num_partitions), cluster.getNum_partitions());
    }
    
    /**
     * Convenience method for launching an HStoreSite for the given Site
     * This will block until the HStoreSite has sucecssfully started
     * @param catalog_site
     * @param hstore_conf
     * @return
     * @throws InterruptedException
     */
    protected final HStoreSite createHStoreSite(Site catalog_site, HStoreConf hstore_conf) throws Exception {
        assert(hasVoltLib()) : "Missing EE shared library";
        
        final CountDownLatch readyLock = new CountDownLatch(1);
        final EventObserver<HStoreSite> ready = new EventObserver<HStoreSite>() {
            @Override
            public void update(EventObservable<HStoreSite> o, HStoreSite arg) {
                readyLock.countDown();
            }
        };
        HStoreSite hstore_site = HStore.initialize(catalogContext, catalog_site.getId(), hstore_conf);
        hstore_site.getReadyObservable().addObserver(ready);
        Thread thread = new Thread(hstore_site);
        thread.start();
        
        // Wait at least 15 seconds or until we know that our HStoreSite has started
        boolean isReady = readyLock.await(15, TimeUnit.SECONDS);
        assertTrue("Failed to start HStoreSite for " + catalog_site, isReady);
        
        // I added an extra little sleep just to be sure...
        ThreadUtil.sleep(3000);

        assertNotNull(hstore_site);
        return (hstore_site);
    }
    
    /**
     * Convenience method for creating a client connection to a random HStoreSite
     * This assumes that the H-Store cluster is up and running properly
     * @return
     * @throws Exception
     */
    protected final Client createClient() throws Exception {
        // Connect to random site and using a random port that it's listening on
        Site catalog_site = CollectionUtil.random(catalogContext.sites);
        assertNotNull(catalog_site);
        
        Host catalog_host = catalog_site.getHost();
        assertNotNull(catalog_host);
        
        String hostName = catalog_host.getIpaddr();
        int port = catalog_site.getProc_port();
        
        LOG.debug(String.format("Creating new client connection to HStoreSite %s at %s:%d",
                                HStoreThreadManager.formatSiteName(catalog_site.getId()),
                                hostName, port));
        
        Client client = ClientFactory.createClient(128, null, false, null);
        client.createConnection(null, hostName, port, "user", "password");
        return (client);
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
        return (type.getProjectFile(new File(".").getCanonicalFile(), "workloads", suffix+".trace"));
    }
    
    /**
     * Find a stats cache file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getStatsFile(ProjectType type) throws IOException {
        return (type.getProjectFile(new File(".").getCanonicalFile(), "stats", ".stats"));
    }
    
    /**
     * Find a parameter correlations file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getParameterMappingsFile(ProjectType type) throws IOException {
        // HACK HACK HACK
        File srcDir = FileUtil.findDirectory("src");
        File mappingsFile = FileUtil.join(srcDir.getAbsolutePath(),
                                         "benchmarks",
                                         type.getPackageName().replace(".", File.separator),
                                         type.name().toLowerCase() + ".mappings");
        return (mappingsFile);
        // return (this.getProjectFile(new File(".").getCanonicalFile(), type, "mappings", ".mappings"));
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
     * Generate an array of random input parameters for a given Statement
     * @param catalog_stmt
     * @return
     */
    protected Object[] randomStatementParameters(Statement catalog_stmt) {
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