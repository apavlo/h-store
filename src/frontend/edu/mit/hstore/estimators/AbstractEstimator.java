package edu.mit.hstore.estimators;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.hashing.AbstractHasher;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ParameterMangler;
import edu.mit.hstore.HStoreSite;

public abstract class AbstractEstimator {
    private static final Logger LOG = Logger.getLogger(AbstractEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    protected final HStoreSite hstore_site;
    protected final AbstractHasher hasher;
    protected final Map<Procedure, ParameterMangler> manglers;
    
    /**
     * PartitionId -> PartitionId Singleton Sets
     */
    protected final Map<Integer, Collection<Integer>> singlePartitionSets = new HashMap<Integer, Collection<Integer>>();
    
    public AbstractEstimator(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
        this.hasher = hstore_site.getHasher();
        this.manglers = hstore_site.getParameterManglers();
        
        for (Integer p : this.hstore_site.getLocalPartitionIds()) {
            this.singlePartitionSets.put(p, Collections.singleton(p));
        } // FOR
    }
    
    public final Collection<Integer> initializeTransaction(Procedure catalog_proc, Object args[]) {
        ParameterMangler mangler = this.manglers.get(catalog_proc);
        if (mangler == null) return (null);
        
        Object mangled[] = mangler.convert(args);
        if (debug.get()) LOG.debug(String.format("Checking %s input parameters:\n%s",
                                                 catalog_proc.getName(), mangler.toString(mangled)));
        return this.initializeTransactionImpl(catalog_proc, args, mangled);
    }
    
    protected abstract Collection<Integer> initializeTransactionImpl(Procedure catalog_proc, Object args[], Object mangled[]);
    
}
