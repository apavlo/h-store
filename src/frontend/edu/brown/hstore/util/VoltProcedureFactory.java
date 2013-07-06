package edu.brown.hstore.util;

import org.apache.log4j.Logger;
import org.voltdb.VoltProcedure;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.PartitionExecutor;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.pools.TypedPoolableObjectFactory;

/**
 * Create a new instance of the corresponding VoltProcedure that is tied
 * to just one PartitionExecutor for the given Procedure catalog object
 */
public class VoltProcedureFactory extends TypedPoolableObjectFactory<VoltProcedure> {
    private static final Logger LOG = Logger.getLogger(VoltProcedureFactory.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final PartitionExecutor executor;
    private final Procedure catalog_proc;
    private final boolean has_java;
    private final Class<? extends VoltProcedure> proc_class;
    
    @SuppressWarnings("unchecked")
    public VoltProcedureFactory(PartitionExecutor executor, Procedure catalog_proc) {
        super(executor.getHStoreConf().site.pool_profiling);
        this.executor = executor;
        this.catalog_proc = catalog_proc;
        this.has_java = this.catalog_proc.getHasjava();
        
        // Only try to load the Java class file for the SP if it has one
        Class<? extends VoltProcedure> p_class = null;
        if (catalog_proc.getHasjava()) {
            final String className = catalog_proc.getClassname();
            try {
                p_class = (Class<? extends VoltProcedure>)Class.forName(className);
            } catch (final ClassNotFoundException e) {
                LOG.fatal("Failed to load procedure class '" + className + "'", e);
                throw new RuntimeException(e);
            }
        }
        this.proc_class = p_class;

    }
    @Override
    public VoltProcedure makeObjectImpl() throws Exception {
        VoltProcedure volt_proc = null;
        try {
            if (this.has_java) {
                volt_proc = (VoltProcedure)this.proc_class.newInstance();
            } else {
                volt_proc = new VoltProcedure.StmtProcedure();
            }
            volt_proc.init(this.executor,
                           this.catalog_proc,
                           this.executor.getBackendTarget());
        } catch (Exception e) {
            LOG.error("Failed to created VoltProcedure instance for " + catalog_proc.getName() , e);
            throw e;
        }
        return (volt_proc);
    }

}
