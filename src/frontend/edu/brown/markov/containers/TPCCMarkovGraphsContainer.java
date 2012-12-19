package edu.brown.markov.containers;

import java.util.Arrays;
import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.markov.MarkovGraph;

public class TPCCMarkovGraphsContainer extends MarkovGraphsContainer {
    private static final Logger LOG = Logger.getLogger(TPCCMarkovGraphsContainer.class);
    private static final boolean d = LOG.isDebugEnabled();

    // private boolean neworder_useInt = false;

    public TPCCMarkovGraphsContainer(Collection<Procedure> procedures) {
        super(procedures);
    }

    @Override
    public MarkovGraph getFromParams(Long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        MarkovGraph ret = null;
        
        String proc_name = catalog_proc.getName();
        int id = -1;
        
        // NEWORDER
        if (proc_name.equals("neworder")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            assert(this.hasher != null) : "Missing hasher!";
            id = this.processNeworder(txn_id, base_partition, params, catalog_proc, 0);
        // PAYMENT
        } else if (proc_name.startsWith("payment")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            assert(this.hasher != null) : "Missing hasher!";
            id = this.processPayment(txn_id, base_partition, params, catalog_proc);
        // DEFAULT
        } else {
            if (d) LOG.debug(String.format("Using default MarkovGraph for %s txn #%d", proc_name, txn_id));
            id = base_partition;
        }
        ret = this.getOrCreate(id, catalog_proc, true);
        assert(ret != null);
        
        return (ret);
    }
    
    public int processNeworder(long txn_id, int base_partition, Object[] params, Procedure catalog_proc, int ctr) {
        // VALUE(D_ID) 
//        int d_id = -1;
//        try {
//            if (this.neworder_useInt) {
//                d_id = ((Integer)params[1]).intValue();
//            } else {
//                d_id = ((Byte)params[1]).intValue();
//            }
//        } catch (ClassCastException e) {
//            if (ctr > 10) {
//                throw e;
//            }
//            this.neworder_useInt = (this.neworder_useInt == false);
//            return (this.processNeworder(txn_id, base_partition, params, catalog_proc, ++ctr));
//        }
        
        // ARRAYLENGTH(S_W_IDS)
        Short arr[] = (Short[])params[5];
        
        // SAMEVALUE(S_W_IDS)
        int other_hash_idx = 0;
//        int other_hash = -1;
        for (int i = 0; i < arr.length; i++) {
            int hash = this.hasher.hash(arr[i]);
            if (i > 0 && base_partition != hash) {
                other_hash_idx = i + 1;
//                other_hash = hash;
                break;
            }
        } // FOR

        if (d) {
            int hashes[] = new int[arr.length];
            for (int i = 0; i < hashes.length; i++) {
                hashes[i] = this.hasher.hash(arr[i]);
            } // FOR
            LOG.debug(String.format("NEWORDER Txn #%d\n" +
                                    // "  VALUE(D_ID) = %d\n" +
                                    "  ARRAYLENGTH(S_W_IDS) = %d / %s\n" +
            		                // "  SAME_VALUE(S_W_IDS) = %d\n"
            		                "  FIRST_DIFF(S_W_IDS) = %d\n",
            		                txn_id,
            		                // d_id,
            		                arr.length, Arrays.toString(hashes),
            		                other_hash_idx));
        }
        
        return (arr.length | other_hash_idx<<8); //  | other_hash<<16);
    }
    
    public int processPayment(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        // HASH(W_ID)
        // int hash_w_id = this.hasher.hash(params[0]);
        
        // HASH(C_W_ID)
        int hash_c_w_id = this.hasher.hash(params[3]);
        
        if (d) LOG.debug(String.format("PAYMENT Txn #%d HASH[C_W_ID] = %d / %s", txn_id, hash_c_w_id, params[3]));
        
        // return (hash_c_w_id);
        return (base_partition | hash_c_w_id<<16);
    }
}
