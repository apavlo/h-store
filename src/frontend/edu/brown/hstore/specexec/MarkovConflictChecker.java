package edu.brown.hstore.specexec;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;

import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimate;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.markov.EstimationThresholds;

/**
 * A more fine-grained ConflictChecker based on estimations of what 
 * rows the txns will read/write.
 * @author pavlo
 */
public class MarkovConflictChecker extends AbstractConflictChecker {
    private static final Logger LOG = Logger.getLogger(MarkovConflictChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final ParameterMappingsSet paramMappings;
    private final EstimationThresholds thresholds;
    private final boolean disabled;
    private final Map<Statement, Map<Statement, ConflictPair>> stmtConflicts = new HashMap<Statement, Map<Statement,ConflictPair>>();
    
    public MarkovConflictChecker(CatalogContext catalogContext, EstimationThresholds thresholds) {
        super(catalogContext);
        this.paramMappings = catalogContext.paramMappings;
        this.thresholds = thresholds;
        this.disabled = (this.paramMappings == null);
    }
    
    private void precomputeConflicts(Procedure proc0) {
        // For each Procedure (including the one we're calling this method for), we 
        // want to compute the minimum number of ConflictParameterPairs that we need
        // to evaluate in order to determine that two transactions will never be
        // conflicting.
        for (Procedure proc1 : this.catalogContext.getRegularProcedures()) {
             Collection<ConflictPair> conflictPairs = ConflictSetUtil.getAllConflictPairs(proc0, proc1);
        }
    }

    @Override
    public boolean ignoreProcedure(Procedure proc) {
        return (this.disabled);
    }

    @Override
    public boolean isConflicting(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
        // Get the queries for both of the txns
        EstimatorState dtxnState = dtxn.getEstimatorState();
        EstimatorState tsState = ts.getEstimatorState();
        if (dtxnState == null || tsState == null) {
            if (debug.get())
                LOG.debug(String.format("No EstimatorState available for %s<->%s", dtxn, ts));
            return (true);
        }
        
        // Get the current TransactionEstimate for the DTXN and the 
        // initial TransactionEstimate for the single-partition txn
        // We need to make sure that both estimates have the list of 
        // queries that the transaction is going to execute
        TransactionEstimate dtxnEst = dtxnState.getLastEstimate();
        assert(dtxnEst != null);
        if (dtxnEst.hasQueryList() == false) {
            if (debug.get())
                LOG.debug(String.format("No query list estimate is available for dtxn %s", dtxn));
            return (true);
        }
        TransactionEstimate tsEst = tsState.getInitialEstimate();
        assert(tsEst != null);
        if (tsEst.hasQueryList() == false) {
            if (debug.get())
                LOG.debug(String.format("No query list estimate is available for candidate %s", ts));
            return (true);
        }
        
        // If both txns are read-only, then we can let our homeboy go
        boolean readonly0 = dtxnEst.isReadOnlyPartition(this.thresholds, partitionId);
        boolean readonly1 = tsEst.isReadOnlyPartition(this.thresholds, partitionId);
        if (readonly0 && readonly1) {
            if (debug.get())
                LOG.debug(String.format("%s<->%s are both are read-only. No conflict!", dtxn, ts));
            return (true);
        }
        
        Collection<ConflictPair> allConflicts = this.getConflicts(dtxnEst.getEstimatedQueries(partitionId),
                                                                  tsEst.getEstimatedQueries(partitionId));
        assert(allConflicts != null);
        
        
        
        
        return false;
    }
    
    private Collection<ConflictPair> getConflicts(Statement dtxnQueries[], Statement tsQueries[]) {
        Set<ConflictPair> allConflicts = new HashSet<ConflictPair>();
        for (int i = 0; i < dtxnQueries.length; i++) {
            Statement stmt0 = dtxnQueries[i];
            Map<Statement, ConflictPair> conflicts = this.stmtConflicts.get(stmt0);
            for (int ii = 0; ii < tsQueries.length; ii++) {
                Statement stmt1 = tsQueries[i];
                ConflictPair cp = conflicts.get(stmt1);
                // If there isn't a ConflictPair, then there isn't a conflict
                if (cp == null) continue;
                allConflicts.add(cp);
            } // FOR
        } // FOR
        return (allConflicts);
    }

}
