package edu.brown.markov.benchmarks;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.CatalogUtil;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovGraphsContainer;
import edu.brown.markov.MarkovUtil;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ThreadUtil;
import edu.brown.workload.TransactionTrace;

public class TPCCMarkovGraphsContainer extends MarkovGraphsContainer {
    private static final Logger LOG = Logger.getLogger(TPCCMarkovGraphsContainer.class);
    private static final boolean d = LOG.isDebugEnabled();
    private static final boolean t = LOG.isTraceEnabled();


    public TPCCMarkovGraphsContainer(Collection<Procedure> procedures) {
        super(procedures);
    }

    @Override
    public MarkovGraph getFromParams(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        MarkovGraph ret = null;
        
        String proc_name = catalog_proc.getName();
        int id = base_partition;
        
        // NEWORDER
        if (proc_name.equals("neworder")) {
            id = this.processNeworder(txn_id, base_partition, params, catalog_proc);
        // PAYMENT
        } else if (proc_name.startsWith("payment")) {
            id = this.processPayment(txn_id, base_partition, params, catalog_proc);
        // DEFAULT
        } else {
            if (t) LOG.trace(String.format("Using default MarkovGraph for %s txn #%d", proc_name, txn_id));
        }
        ret = this.getOrCreate(id, catalog_proc, true);
        assert(ret != null);
        
        return (ret);
    }
    
    public int processNeworder(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        // VALUE(D_ID) 
        int d_id = ((Long)params[1]).intValue();
        
        // ARRAYLENGTH[S_W_IDS]
        int arr_len = ((Object[])params[5]).length;
        
        if (t) {
            Object arr[] = (Object[])params[5];
            int hashes[] = new int[arr.length];
            for (int i = 0; i < hashes.length; i++) {
                hashes[i] = this.hasher.hash(arr[i]);
            }
            LOG.trace(String.format("NEWORDER Txn #%d\n  ARRAYLENGTH[S_W_IDS] = %d / %s\n  VALUE(D_ID) = %d ", txn_id, arr_len, Arrays.toString(hashes), d_id));
        }
        
        // return (arr_len);
        return (d_id | arr_len<<16);
    }
    
    public int processPayment(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        // HASH(W_ID)
        // int hash_w_id = this.hasher.hash(params[0]);
        
        // HASH(C_W_ID)
        int hash_c_w_id = this.hasher.hash(params[3]);
        
        if (t) LOG.info(String.format("PAYMENT Txn #%d HASH[C_W_ID] = %d / %s", txn_id, hash_c_w_id, params[3]));
        
        return (hash_c_w_id);
        // return (hash_w_id | hash_c_w_id<<16);
    }
    
    
    /**
     * 
     * @param vargs
     * @throws Exception
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        create(args);
    }
    
    /**
     * Create the MarkovGraphsContainers for the given workload
     * @param args
     * @throws Exception
     */
    public static void create(final ArgumentsParser args) throws Exception {
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_MARKOV_OUTPUT
        );
        LOG.info("Calculating TPC-C MarkovGraphsContainer for " + CatalogUtil.getNumberOfPartitions(args.catalog_db) + " partitions");
        
        final PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        final Map<Integer, TPCCMarkovGraphsContainer> markovs_map = new ConcurrentHashMap<Integer, TPCCMarkovGraphsContainer>();
        
        List<Runnable> runnables = new ArrayList<Runnable>();
        final Set<Procedure> procedures = args.workload.getProcedures(args.catalog_db);
        final AtomicInteger ctr = new AtomicInteger(0);
        final Histogram<Procedure> proc_h = new Histogram<Procedure>();
        
        for (final Procedure catalog_proc : procedures) {
            runnables.add(new Runnable() {
                public void run() {
                    List<TransactionTrace> traces = args.workload.getTraces(catalog_proc); 
                    LOG.info(String.format("Populating global MarkovGraph for %s [#traces=%d]", catalog_proc.getName(), traces.size()));
                    for (TransactionTrace txn_trace : traces) {
                        long txn_id = txn_trace.getTransactionId();
                        try {
                            int base_partition = p_estimator.getBasePartition(txn_trace);
                            Object params[] = txn_trace.getParams();
                            Procedure catalog_proc = txn_trace.getCatalogItem(args.catalog_db);
                            
                            TPCCMarkovGraphsContainer markovs = markovs_map.get(base_partition);
                            if (markovs == null) {
                                synchronized (markovs_map) {
                                    markovs = markovs_map.get(base_partition);
                                    if (markovs == null) {
                                        markovs = new TPCCMarkovGraphsContainer(procedures);
                                        markovs.setHasher(p_estimator.getHasher());
                                        markovs_map.put(base_partition, markovs);
                                    }
                                } // SYNCH
                            }
                            
                            MarkovGraph markov = markovs.getFromParams(txn_id, base_partition, params, catalog_proc);
                            markov.processTransaction(txn_trace, p_estimator);
                        } catch (Exception ex) {
                            LOG.fatal("Failed to process " + txn_trace, ex);
                            throw new RuntimeException(ex);
                        }
                        proc_h.put(catalog_proc);
                    } // FOR
                    LOG.info(String.format("Finished creating MarkovGraphs for %s [%d/%d]", catalog_proc.getName(), ctr.incrementAndGet(), procedures.size()));
                }
            });
        } // FOR
        ThreadUtil.runGlobalPool(runnables);

        LOG.info("Procedure Histogram:\n" + proc_h);
        MarkovUtil.calculateProbabilities(markovs_map);
        
        String output = args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT);
        MarkovUtil.save(markovs_map, output);
        LOG.info("Wrote Markov to " + output);
    }
    
}
