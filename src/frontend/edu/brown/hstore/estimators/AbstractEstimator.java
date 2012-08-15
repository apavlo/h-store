package edu.brown.hstore.estimators;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Procedure;

import edu.brown.hashing.AbstractHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ParameterMangler;
import edu.brown.utils.PartitionSet;

public abstract class AbstractEstimator {
    private static final Logger LOG = Logger.getLogger(AbstractEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    protected final CatalogContext catalogContext;
    protected final AbstractHasher hasher;
    protected final Map<Procedure, ParameterMangler> manglers;
    
    /**
     * PartitionId -> PartitionId Singleton Sets
     */
    protected final Map<Integer, PartitionSet> singlePartitionSets = new HashMap<Integer, PartitionSet>();
    
    public AbstractEstimator(CatalogContext catalogContext, Map<Procedure, ParameterMangler> manglers, AbstractHasher hasher) {
        this.catalogContext = catalogContext;
        this.hasher = hasher;
        this.manglers = manglers;
        
        for (Integer p : catalogContext.getAllPartitionIdArray()) {
            this.singlePartitionSets.put(p, new PartitionSet(p));
        } // FOR
        
        if (debug.get())
            LOG.debug("Initialized fixed transaction estimator -> " + this.getClass().getSimpleName());
    }
    
    public final PartitionSet initializeTransaction(Procedure catalog_proc, Object args[]) {
        ParameterMangler mangler = this.manglers.get(catalog_proc);
        if (mangler == null) return (null);
        
        Object mangled[] = mangler.convert(args);
        if (debug.get()) LOG.debug(String.format("Checking %s input parameters:\n%s",
                                                 catalog_proc.getName(), mangler.toString(mangled)));
        return this.initializeTransactionImpl(catalog_proc, args, mangled);
    }
    
    /**
     * Returns the list of partitions that we think that this txn is going to
     * need to access.
     * @param catalog_proc
     * @param args
     * @param mangled
     * @return
     */
    protected abstract PartitionSet initializeTransactionImpl(Procedure catalog_proc, Object args[], Object mangled[]);
    
}
