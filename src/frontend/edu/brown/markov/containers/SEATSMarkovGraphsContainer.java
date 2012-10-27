package edu.brown.markov.containers;

import java.util.Collection;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.markov.MarkovGraph;

public class SEATSMarkovGraphsContainer extends MarkovGraphsContainer {
    private static final Logger LOG = Logger.getLogger(SEATSMarkovGraphsContainer.class);
    private static final boolean d = LOG.isDebugEnabled();

    public SEATSMarkovGraphsContainer(Collection<Procedure> procedures) {
        super(procedures);
    }

    @Override
    public MarkovGraph getFromParams(Long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        assert(this.hasher != null) : "Missing hasher!";
        MarkovGraph ret = null;
        
        String proc_name = catalog_proc.getName();
        int id = -1;
        
        // NewReservation
        if (proc_name.equals("NewReservation")) {
            if (d) LOG.debug(String.format("Selecting MarkovGraph using decision tree for %s txn #%d", proc_name, txn_id));
            id = this.processNewReservation(txn_id, base_partition, params, catalog_proc);
            
        // DEFAULT
        } else {
            if (d) LOG.debug(String.format("Using default MarkovGraph for %s txn #%d", proc_name, txn_id));
            id = base_partition;
        }
        ret = this.getOrCreate(id, catalog_proc, true);
        assert(ret != null);
        
        return (ret);
    }
    
    public int processDeleteReservation(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        int use_c_id_str = 0;
        int use_ff_c_id_str = 0;
        
        // C_ID == NULL
        long c_id = ((Long)params[1]).longValue();
        if (c_id == VoltType.NULL_BIGINT) {
            if (params[2] != null) {
                use_c_id_str = 1;
            } else {
                use_ff_c_id_str = 1;
            }
        }
        
        return (use_c_id_str | use_ff_c_id_str<<8);
    }
    
    private int processNewReservation(long txn_id, int base_partition, Object[] params, Procedure catalog_proc) {
        // HASHVALUE(F_ID) 
        int id = base_partition;
        
        // UPDATE CUSTOMER
        if (((Long)params[1]).longValue() == VoltType.NULL_BIGINT) {
            id |= 1<<31;
        }
        
        return (id);
    }
}
