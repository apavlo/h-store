/**
 * 
 */
package edu.brown.benchmark;

import java.io.File;
import java.io.IOException;
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

import edu.brown.utils.ClassUtil;
import edu.brown.utils.ProjectType;

/**
 * @author pavlo
 *
 */
public abstract class AbstractProjectBuilder extends VoltProjectBuilder {
    private static final Logger LOG = Logger.getLogger(AbstractProjectBuilder.class);

    protected final Class<? extends AbstractProjectBuilder> base_class;
    protected final Class<?> procedures[];
    protected final Class<?> supplementals[];
    protected final String partitioning[][];
    
    private final URL ddlURL;
    private final URL ddlFkeysURL;
    
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
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][]) {
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
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][], Class<?> supplementals[], boolean fkeys) {
        super(project_name);
        this.base_class = base_class;
        this.procedures = procedures;
        this.partitioning = partitioning;
        this.supplementals = supplementals;
        
        this.ddlFkeysURL = this.base_class.getResource(this.getDDLName(true));
        if (fkeys) {
            this.ddlURL = this.ddlFkeysURL;
        } else {
            this.ddlURL = this.base_class.getResource(this.getDDLName(false));
        }
        
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
    
    
    public final String getDDLName(boolean fkeys) {
//        return (this.project_name + "-ddl" + (fkeys ? "-fkeys" : "") + ".sql");
        return (this.project_name + "-ddl" + ".sql");
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
    
    /**
     * Get the full jar path for this project
     * @param unitTest
     * @return
     */
    public final File getJarPath(boolean unitTest) {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        return (new File(testDir + File.separator + this.getJarName(unitTest)));
    }
    
    public void addPartitions() {
        for (String i[] : this.partitioning) {
            addPartitionInfo(i[0], i[1]);
        } // FOR
    }
    
    @Override
    public void addAllDefaults() {
        addProcedures(this.procedures);
        addSchema(this.ddlURL);
        addPartitions();
    }
    
    /**
     * Get a pointer to a compiled catalog for the benchmark for all the procedures.
     */
    public Catalog createCatalog(boolean fkeys, boolean full_catalog) throws IOException {
        // compile a catalog
        if (full_catalog) {
            this.addProcedures(this.procedures);    
        } else {
            // The TPC-E catalog takes a long time load, so we have the ability
            // to just compile the schema and the first procedure to make things load faster
            this.addProcedures(this.procedures[0]);
        }
        addSchema(fkeys ? this.ddlFkeysURL : this.ddlURL);
        addPartitions();

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
        LOG.debug("Dynamically creating project builder for " + type + ": " + pb_className);
        final AbstractProjectBuilder pb = (AbstractProjectBuilder)ClassUtil.newInstance(pb_className,
                                                   new Object[]{  }, new Class<?>[]{  });
        assert(pb != null) : "Invalid ProjectType " + type;
        return (pb);
    }
}