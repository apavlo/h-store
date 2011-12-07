package edu.mit.hstore.estimators;

import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Procedure;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.mit.hstore.HStoreSite;

public class SEATSEstimator extends AbstractEstimator {
    private static final Logger LOG = Logger.getLogger(SEATSEstimator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    /**
     * Constructor
     * @param hstore_site
     */
    public SEATSEstimator(HStoreSite hstore_site) {
        super(hstore_site);
    }
    
    @Override
    protected Collection<Integer> initializeTransactionImpl(Procedure catalog_proc, Object args[], Object mangled[]) {
        String procName = catalog_proc.getName();
        long f_id = VoltType.NULL_BIGINT;
        long c_id = VoltType.NULL_BIGINT;
        Collection<Integer> ret = null;
        
        if (procName.equalsIgnoreCase("NewReservation") ||
            procName.equalsIgnoreCase("UpdateReservation")) {
            c_id = (Long)mangled[1];
            f_id = (Long)mangled[2];
        }
        else if (procName.equalsIgnoreCase("DeleteReservation")) {
            c_id = (Long)mangled[1];
            f_id = (Long)mangled[0];
        }
        else if (procName.equalsIgnoreCase("FindOpenSeats")) {
            f_id = (Long)mangled[0];
        }
        else if (procName.equalsIgnoreCase("UpdateCustomer")) {
            c_id = (Long)mangled[0];
        }
        
        // Construct partitions collection!
        if (f_id != VoltType.NULL_BIGINT && c_id != VoltType.NULL_BIGINT) {
            ret = new HashSet<Integer>();
            ret.add(hasher.hash(f_id));
            ret.add(hasher.hash(c_id));
        }
        else if (f_id != VoltType.NULL_BIGINT) {
            ret = this.singlePartitionSets.get(hasher.hash(f_id));
        }
        else if (c_id != VoltType.NULL_BIGINT) {
            ret = this.singlePartitionSets.get(hasher.hash(c_id));    
        }
        else {
            ret = hstore_site.getAllPartitionIds();
        }

        return (ret);
    }
}
