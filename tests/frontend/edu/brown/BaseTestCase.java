package edu.brown;

import junit.framework.TestCase;

import java.io.File;
import java.io.IOException;
import java.util.*;

import org.voltdb.VoltProcedure;
import org.voltdb.catalog.*;
import org.voltdb.benchmark.tpcc.TPCCProjectBuilder;

import edu.brown.benchmark.airline.AirlineProjectBuilder;
import edu.brown.benchmark.auctionmark.AuctionMarkProjectBuilder;
import edu.brown.benchmark.markov.MarkovProjectBuilder;
import edu.brown.benchmark.tm1.TM1ProjectBuilder;
import edu.brown.benchmark.tpce.TPCEProjectBuilder;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.FixCatalog;
import edu.brown.catalog.ParametersUtil;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ProjectType;

public abstract class BaseTestCase extends TestCase {

    // log4j Hack
    static {
        ArgumentsParser.setupLogging();
    }
    
    protected ProjectType last_type;
    
    protected static Catalog catalog;
    protected static final Map<ProjectType, Catalog> project_catalogs = new HashMap<ProjectType, Catalog>();
    
    protected static Database catalog_db;
    protected static final Map<ProjectType, Database> project_databases = new HashMap<ProjectType, Database>();

    protected static PartitionEstimator p_estimator;
    protected static final Map<ProjectType, PartitionEstimator> project_p_estimators = new HashMap<ProjectType, PartitionEstimator>();
    
    protected void setUp(ProjectType type) throws Exception {
        this.setUp(type, false);
    }
    
    protected void setUp(ProjectType type, boolean fkeys) throws Exception {
        this.setUp(type, fkeys, true);
    }
    
    protected void setUp(ProjectType type, boolean fkeys, boolean full_catalog) throws Exception {
        super.setUp();
        this.last_type = type;
        catalog = project_catalogs.get(type);
        catalog_db = project_databases.get(type);
        p_estimator = project_p_estimators.get(type);
        if (catalog == null) {
            switch (type) {
                case TPCC:
                    catalog = TPCCProjectBuilder.getTPCCSchemaCatalog(fkeys);
                    // Update the ProcParameter mapping used in the catalogs
                    ParametersUtil.populateCatalog(CatalogUtil.getDatabase(catalog), ParametersUtil.getParameterMapping(type));
                    break;
                case TPCE:
                    catalog = new TPCEProjectBuilder().createCatalog(fkeys, full_catalog);
                    break;
                case TM1:
                    catalog = new TM1ProjectBuilder().getFullCatalog(fkeys);
                    break;
                case AIRLINE:
                    catalog = new AirlineProjectBuilder().getFullCatalog(fkeys);
                    break;
                case AUCTIONMARK:
                    catalog = new AuctionMarkProjectBuilder().getFullCatalog(fkeys);
                    break;
                case MARKOV:
                    catalog = new MarkovProjectBuilder().getFullCatalog(fkeys);
                    break;
                default:
                    assert(false) : "Invalid project type - " + type;
            } // SWITCH
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
    
    protected Cluster getCluster() {
        assertNotNull(catalog);
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        assert(catalog_clus != null) : "Failed to retriever cluster object from catalog";
        return (catalog_clus);
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
        Column catalog_col = catalog_tbl.getColumns().get(col_name);
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

    protected Procedure getProcedure(Database catalog_db, String proc_name) {
        assertNotNull(catalog_db);
        Procedure catalog_proc = catalog_db.getProcedures().get(proc_name);
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
            Vector<String[]> triplets = new Vector<String[]>();
            for (Integer i = 0; i < num_partitions; i++) {
                triplets.add(new String[] {
                    "localhost",
                    Integer.toString(1000 + i),
                    i.toString(),
                });
                System.err.println("[" + i + "] " + Arrays.toString(triplets.lastElement()));
            } // FOR
            catalog = FixCatalog.addHostInfo(catalog, triplets);
            this.init(this.last_type, catalog);
            
        }
        Cluster cluster = CatalogUtil.getCluster(catalog_db);
        assertEquals(num_partitions, cluster.getNum_partitions());
        assertEquals(num_partitions, CatalogUtil.getNumberOfPartitions(cluster));
    }

    /**
     * Find a trace file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getWorkloadFile(ProjectType type) throws IOException {
        return (this.getWorkloadFile(new File(".").getCanonicalFile(), type, "workloads", "trace"));
    }
    
    /**
     * Find a stats cache file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getStatsFile(ProjectType type) throws IOException {
        return (this.getWorkloadFile(new File(".").getCanonicalFile(), type, "workloads", "stats"));
    }
    
    /**
     * Find a parameter correlations file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getCorrelationsFile(ProjectType type) throws IOException {
        return (this.getWorkloadFile(new File(".").getCanonicalFile(), type, "correlations", "correlations"));
    }
    
    /**
     * Find a Markov file for a given project type
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    public File getMarkovFile(ProjectType type) throws IOException {
        return (this.getWorkloadFile(new File(".").getCanonicalFile(), type, "markovs", "markovs"));
    }
    
    /**
     * 
     * @param current
     * @param type
     * @return
     * @throws IOException
     */
    private File getWorkloadFile(File current, ProjectType type, String target_dir, String target_ext) throws IOException {
        boolean has_svn = false;
        for (File file : current.listFiles()) {
            if (file.getCanonicalPath().endsWith("files") && file.isDirectory()) {
                // Look for either a .trace or a .trace.gz file
                for (int i = 0; i < 2; i++) {
                    String file_name = type.name().toLowerCase() + "." + target_ext;
                    if (i > 0) file_name += ".gz";
                    File target_file = new File(file + File.separator + target_dir + File.separator + file_name);
                    if (target_file.exists() && target_file.isFile()) {
                        return (target_file);
                    }
                } // FOR
                assert(false) : "Unable to find '" + target_ext + "' file for '" + type + "' in directory '" + file + "'";
            // Make sure that we don't go to far down...
            } else if (file.getCanonicalPath().endsWith("/.svn")) {
                has_svn = true;
            }
        } // FOR
        assert(has_svn) : "Unable to find files directory [last_dir=" + current.getAbsolutePath() + "]";  
        File next = new File(current.getCanonicalPath() + File.separator + "..");
        return (this.getWorkloadFile(next, type, target_dir, target_ext));
    }
    
}
