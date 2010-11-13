/**
 * 
 */
package edu.brown.benchmark;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.voltdb.catalog.Catalog;
import org.voltdb.compiler.VoltProjectBuilder;
import org.voltdb.utils.BuildDirectoryUtils;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.JarReader;

/**
 * @author pavlo
 *
 */
public abstract class AbstractProjectBuilder extends VoltProjectBuilder {

    protected final Class<? extends AbstractProjectBuilder> base_class;
    protected final String project_name;
    protected final Class<?> procedures[];
    protected final Class<?> supplementals[];
    protected final String partitioning[][];
    
    protected final URL ddlURL;
    protected final URL ddlFkeysURL;

    /**
     * Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][]) {
        this(project_name, base_class, procedures, partitioning, new Class<?>[0], false);
    }
    
    /**
     * Constructor
     * @param project_name
     * @param base_class
     * @param procedures
     * @param partitioning
     * @param supplementals
     */
    public AbstractProjectBuilder(String project_name, Class<? extends AbstractProjectBuilder> base_class, Class<?> procedures[], String partitioning[][], Class<?> supplementals[]) {
       this(project_name, base_class, procedures, partitioning, supplementals, false);
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
        super();
        this.project_name = project_name;
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
    
    public String getDDLName(boolean fkeys) {
        return (this.project_name + "-ddl" + (fkeys ? "-fkeys" : "") + ".sql");
    }
    
    public String getJarName() {
        return (this.project_name + "-jni.jar");
    }
    public File getJarPath() {
        String testDir = BuildDirectoryUtils.getBuildDirectoryPath();
        return (new File(testDir + File.separator + this.getJarName()));
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

        String catalogJar = this.getJarPath().getAbsolutePath();
        try {
            boolean status = compile(catalogJar);
            assert (status);
        } catch (Exception ex) {
            throw new RuntimeException("Failed to " + project_name + " catalog [" + catalogJar + "]", ex);
        }

        // read in the catalog
        String serializedCatalog = JarReader.readFileFromJarfile(catalogJar, CatalogUtil.CATALOG_FILENAME);

        // create the catalog (that will be passed to the ClientInterface
        Catalog catalog = new Catalog();
        catalog.execute(serializedCatalog);

        return catalog;
    }
    
    public Catalog getFullCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, true));
    }
    
    public Catalog getSchemaCatalog(boolean fkeys) throws IOException {
        return (this.createCatalog(fkeys, false));
    }
}
