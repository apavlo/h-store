package edu.brown.markov.containers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.markov.MarkovGraph;

public class AuctionMarkMarkovGraphsContainer extends MarkovGraphsContainer {
    private static final Logger LOG = Logger.getLogger(AuctionMarkMarkovGraphsContainer.class);
    private static final boolean d = LOG.isDebugEnabled();

    public AuctionMarkMarkovGraphsContainer(Collection<Procedure> procedures) {
        super(procedures);
    }

    @Override
    public MarkovGraph getFromParams(Long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        assert(this.hasher != null) : "Missing hasher!";
        MarkovGraph ret = null;
        
        String proc_name = catalog_proc.getName();
        int id = -1;
        
        // GETUSERINFO
        if (proc_name.equals("GetUserInfo")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            id = this.processGetUserInfo(txn_id, base_partition, params, catalog_proc);
            
        // NewBid + NewComment + NewFeedback
        } else if (proc_name.equalsIgnoreCase("NewBid") ||
                   proc_name.equalsIgnoreCase("NewComment") ||
                   proc_name.equalsIgnoreCase("NewFeeback")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            id = this.processBuyerSellerId(txn_id, base_partition, params, catalog_proc, 1);
            
        // NewPurchase
        } else if (proc_name.equalsIgnoreCase("NewPurchase")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            id = this.processBuyerSellerId(txn_id, base_partition, params, catalog_proc, 2);
            
        // DEFAULT
        } else {
            if (d) LOG.debug(String.format("Using default MarkovGraph for %s txn #%d", proc_name, txn_id));
            id = base_partition;
        }
        ret = this.getOrCreate(id, catalog_proc, true);
        assert(ret != null);
        
        return (ret);
    }
    
    public int processGetUserInfo(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        // HASHVALUE(U_ID) 
        int id = this.hasher.hash(params[0]);
        
        // BITMASK(params[1:]
        for (int i = 1; i < params.length; i++) {
            id |= ((Long)params[i]).intValue()<<20+i;
        } // FOR
        
        return (id);
    }
    
    public int processBuyerSellerId(long txn_id, int base_partition, Object[] params, Procedure catalog_proc, int offset) {
        // HASHVALUE(SELLER_ID) 
        int seller_id = this.hasher.hash(params[offset]);
        
        // HASHVALUE(BUYER_ID)
        int buyer_id = this.hasher.hash(params[offset+1]);
        
        return (seller_id | buyer_id<<16);
    }
}
