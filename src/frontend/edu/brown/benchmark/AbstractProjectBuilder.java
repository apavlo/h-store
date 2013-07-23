/**
 * 
 */
package edu.brown.benchmark;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.JarReader;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ClassUtil;
import edu.brown.utils.FileUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 */
public abstract class AbstractProjectBuilder extends VoltProjectBuilder {
    private static final Logger LOG = Logger.getLogger(AbstractProjectBuilder.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    protected final Class<? extends AbstractProjectBuilder> base_class;
    protected final Class<? extends VoltProcedure> procedures[];
    protected final Class<?> supplementals[];
    protected final String partitioning[][];
    
    private URL ddlURL;
    protected final File parameterMappings;
    
    protected final TransactionFrequencies txn_frequencies = new TransactionFrequencies();

    public static class TransactionFrequencies extends HashMap<Class<? extends VoltProcedure>, Integer> {
        private static final long serialVersionUID = 1L;
        
        public int getTotal() {
            int total = 0;
            for (Integer freq : this.values()) {
                assert(freq >= 0);
                total += freq;
            } // FOR
            return (total);
        }
    }
    
    /**
     * Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<? extends VoltProcedure> procedures[], String partitioning[][]) {
        this(project_name, base_class, procedures, partitioning, new Class<?>[0], true);
    }
    
    /**
     * Full Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     * @param supplementals
     * @param fkeys
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<? extends VoltProcedure> procedures[], String partitioning[][], Class<?> supplementals[], boolean fkeys) {
        super(project_name);
        this.base_class = base_class;
        this.procedures = procedures;
        this.partitioning = partitioning;
        this.supplementals = supplementals;
        
        this.ddlURL = this.base_class.getResource(this.getDDLName(true));
        this.parameterMappings = this.getParameterMappings();
    }
    
    public void addTransactionFrequency(Class<? extends VoltProcedure> procClass, int frequency) {
        this.txn_frequencies.put(procClass, frequency);
    }
    public String getTransactionFrequencyString() {
        StringBuilder sb = new StringBuilder();
        String add = "";
        for (Entry<Class<? extends VoltProcedure>, Integer> e : txn_frequencies.entrySet()) {
            sb.append(add).append(e.getKey().getSimpleName()).append(":").append(e.getValue());
            add = ",";
        }
        return (sb.toString());
    }
    
    /**
     * Use the given DDL string as the schema for this project
     * This will cause the ProjectBuilder to write DDL out to a temporary file
     * @param ddl
     */
    public File setDDLContents(String ddl) {
        File f = FileUtil.writeStringToTempFile(ddl, "sql", true);
        try {
            this.ddlURL = f.toURI().toURL();
        } catch (MalformedURLException ex) {
            throw new RuntimeException(ex);
        }
        if (debug.val)
            LOG.debug("Wrote DDL contents to '" + f.getAbsolutePath() + "'");
        return (f);
    }
    
    public final URL getDDLURL(boolean fkeys) {
        return (this.ddlURL);
    }
    public final String getDDLName(boolean fkeys) {
//        return (this.project_name + "-ddl" + (fkeys ? "-fkeys" : "") + ".sql");
        return (this.project_name + "-ddl" + ".sql");
    }
    
    public final File getParameterMappings() {
        File file = null;
        String name = this.project_name + ".mappings";
        URL url = this.base_class.getResource(name);
        if (debug.val)
            LOG.debug(this.project_name.toUpperCase() + ": " + url);
        if (url != null) {
            file = new File(url.getPath());
        }
        return (file);
    }
    
    /**
     * Get the base file name for this benchmark's project jar
     * The file name will include a test suffix if it is being used in unit tests
     * @param unitTest
     * @return
     */
    public final String getJarName(boolean unitTest) {
        return (this.project_name + (unitTest ? "-test" : "") + ".jar");
    }
    
    public String getJarDirectory() {
       return BuildDirectoryUtils.getBuildDirectoryPath(); 
    }
    
    /**
     * Get the full jar path for this project
     * @param unitTest
     * @return
     */
    public final File getJarPath(boolean unitTest) {
        String testDir = this.getJarDirectory();
        return (new File(testDir + File.separator + this.getJarName(unitTest)));
    }
    
    public final void addDefaultProcedures() {
        addProcedures(this.procedures);
    }
    
    /**
     * Add the default schema to this project.
     */
    public final void addDefaultSchema() {
        addSchema(this.ddlURL);
    }
    
    /**
     * Add the default partitioning to this project
     */
    public final void addDefaultPartitioning() {
        if (this.partitioning != null && this.partitioning.length > 0) {
            for (String i[] : this.partitioning) {
                addTablePartitionInfo(i[0], i[1]);
            } // FOR
        }
    }
    
    @Override
    public void addAllDefaults() {
        addDefaultProcedures();
        addDefaultSchema();
        addDefaultPartitioning();
        if (this.parameterMappings != null)
            addParameterMappings(this.parameterMappings);
    }
    
    /**
     * Get a pointer to a compiled catalog for the benchmark for all the procedures.
     */
    public Catalog createCatalog() throws IOException {
        return createCatalog(true, true);
    }
    
    /**
     * 
     * @param fkeys
     * @param full_catalog
     * @return
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    public Catalog createCatalog(boolean fkeys, boolean full_catalog) throws IOException {
        // compile a catalog
        if (full_catalog) {
             this.addProcedures(this.procedures);    
        } else {
            // The TPC-E catalog takes a long time load, so we have the ability
            // to just compile the schema and the first procedure to make things load faster
            this.addProcedures(this.procedures[0]);
        }
        addDefaultSchema();
        addDefaultPartitioning();
        if (this.parameterMappings != null)
            addParameterMappings(this.parameterMappings);

        String catalogJar = this.getJarPath(true).getAbsolutePath();
        try {
            boolean status = compile(catalogJar);
            assert (status);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to create " + project_name + " catalog [" + catalogJar + "]", ex);
        }

        Catalog catalog = new Catalog();
        try {
            // read in the catalog
            String serializedCatalog = JarReader.readFileFromJarfile(catalogJar, CatalogUtil.CATALOG_FILENAME);
    
            // create the catalog (that will be passed to the ClientInterface
            catalog.execute(serializedCatalog);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to load " + project_name + " catalog [" + catalogJar + "]", ex);
        }

        return catalog;
    }
    
    public Catalog getFullCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, true));
    }
    
    public Catalog getSchemaCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, false));
    }
    
    public static AbstractProjectBuilder getProjectBuilder(ProjectType type) {
        String pb_className = String.format("%s.%sProjectBuilder", type.getPackageName(), type.getBenchmarkPrefix());
        if (debug.val)
            LOG.debug("Dynamically creating project builder for " + type + ": " + pb_className);
        final AbstractProjectBuilder pb = (AbstractProjectBuilder)ClassUtil.newInstance(pb_className,
                                                   new Object[]{  }, new Class<?>[]{  });
        assert(pb != null) : "Invalid ProjectType " + type;
        return (pb);
    }
}