package edu.brown.workload;

import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.utils.ArgumentsParser;

public abstract class VerifyWorkload {
    private static final Logger LOG = Logger.getLogger(VerifyWorkload.class);
    
    public static boolean verify(Database catalog_db, Workload workload) throws Exception {
        Set<Long> trace_ids = new HashSet<Long>();
        Set<String> txn_ids = new HashSet<String>();
        
        long ctr = 0;
        for (AbstractTraceElement<?> element : workload) {
            long trace_id = element.getId();
            if (trace_ids.contains(trace_id)) {
                LOG.fatal("Duplicate Trace Id: " + element);
                return (false);
            }
            trace_ids.add(trace_id);
            
            // TransactionTrace
            if (element instanceof TransactionTrace) {
                TransactionTrace txn_trace = (TransactionTrace)element;
                String txn_id = txn_trace.getTransactionId();
                if (txn_ids.contains(txn_id)) {
                    LOG.fatal("Duplicate Txn Id: " + txn_id);
                    return (false);
                }
                txn_ids.add(txn_id);
            }
            ctr++;
            if (ctr > 1 && ctr % 1000 == 0) LOG.info("Examined " + ctr + " trace elements...");  
        } // WHILE
        if (txn_ids.isEmpty()) {
            LOG.fatal("No txns were found in workload?");
            return (false);
        } else if (txn_ids.size() != workload.getTransactionCount()) {
            LOG.fatal("Expected to get " + workload.getTransactionCount() + " txns, but we saw " + txn_ids.size());
            return (false);
        }
        
        return (true);
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD
        );
        assert(VerifyWorkload.verify(args.catalog_db, args.workload));
        LOG.info("The workload " + args.workload + " is valid!");
    }

}
