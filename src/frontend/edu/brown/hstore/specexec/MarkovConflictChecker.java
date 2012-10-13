package edu.brown.hstore.specexec;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.conflicts.ConflictParameterPair;
import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.designer.ColumnSet;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.estimators.TransactionEstimate;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
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
    private final IdentityHashMap<ConflictPair, ConflictParameterPair> conflictParams = new IdentityHashMap<ConflictPair, ConflictParameterPair>();

    // ------------------------------------------------------------
    // PRECOMPUTED CACHE
    // ------------------------------------------------------------
    
    /**
     * For each Statement, we have maintain a list of the StmtParameters
     * that are used in predicates with the target table's primary key
     */
    private final Map<Statement, Map<Table, StmtParameter[]>> stmtParameterCache = new HashMap<Statement, Map<Table, StmtParameter[]>>();
    
    /**
     * Table -> Primary Keys
     */
    private final Map<Table, Column[]> pkeysCache = new HashMap<Table, Column[]>();
    
    private final Map<Statement, Table[]> stmtTablesCache = new HashMap<Statement, Table[]>();
    
    
    // ------------------------------------------------------------
    // INITIALIZATION
    // ------------------------------------------------------------
    
    public MarkovConflictChecker(CatalogContext catalogContext, EstimationThresholds thresholds) {
        super(catalogContext);
        this.paramMappings = catalogContext.paramMappings;
        this.thresholds = thresholds;
        this.disabled = (this.paramMappings == null);
        
        for (Table catalog_tbl : CatalogUtil.getDataTables(this.catalogContext.database)) {
            this.pkeysCache.put(catalog_tbl, CatalogUtil.getPrimaryKeyColumns(catalog_tbl).toArray(new Column[0]));
        } // FOR (table)
        for (Procedure proc : this.catalogContext.getRegularProcedures()) {
            for (Statement stmt : proc.getStatements()) {
//                ColumnSet cset = CatalogUtil.extractStatementColumnSet(stmt, false);
//                Map<Table, StmtParameter[]> tableParams = new HashMap<Table, StmtParameter[]>();
//                for (Table tbl : CatalogUtil.getReferencedTables(stmt)) {
//                    for (Column col : this.pkeysCache.get(tbl)) {
//                        Collection<StmtParameter> params = cset.findAllForOther(StmtParameter.class, col);
//                        this.stmtParameterCache.put(stmt)
//                        int offsets[] = new int[params.size()];
//                        int i = 0;
//                        for (StmtParameter )
//                    }
//                }
                
                // this.stmtTablesCache.put(stmt, .toArray(new Table[0]));
            } // FOR (stmt)
        } // FOR (proc)
    }
    
    private void precomputeConflicts(Procedure proc0) {
        // For each Procedure (including the one we're calling this method for), we 
        // want to compute the minimum number of ConflictParameterPairs that we need
        // to evaluate in order to determine that two transactions will never be
        // conflicting.
        for (Procedure proc1 : this.catalogContext.getRegularProcedures()) {
             Collection<ConflictPair> conflicts = ConflictSetUtil.getAllConflictPairs(proc0, proc1);
             for (ConflictPair cp : conflicts) {
                 
             }
             
        }
    }

    @Override
    public boolean ignoreProcedure(Procedure proc) {
        return (this.disabled);
    }

    @Override
    public boolean canExecute(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
        // Get the queries for both of the txns
        EstimatorState dtxnState = dtxn.getEstimatorState();
        EstimatorState tsState = ts.getEstimatorState();
        if (dtxnState == null || tsState == null) {
            if (debug.get())
                LOG.debug(String.format("No EstimatorState available for %s<->%s", dtxn, ts));
            return (false);
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
            return (false);
        }
        TransactionEstimate tsEst = tsState.getInitialEstimate();
        assert(tsEst != null);
        if (tsEst.hasQueryList() == false) {
            if (debug.get())
                LOG.debug(String.format("No query list estimate is available for candidate %s", ts));
            return (false);
        }
        
        // If both txns are read-only, then we can let our homeboy go
        boolean readonly0 = dtxnEst.isReadOnlyPartition(this.thresholds, partitionId);
        boolean readonly1 = tsEst.isReadOnlyPartition(this.thresholds, partitionId);
        if (readonly0 && readonly1) {
            if (debug.get())
                LOG.debug(String.format("%s<->%s are both are read-only. No conflict!", dtxn, ts));
            return (false);
        }
        
        Collection<ConflictPair> allConflicts = this.getConflicts(dtxnEst.getEstimatedQueries(partitionId),
                                                                  tsEst.getEstimatedQueries(partitionId));
        assert(allConflicts != null);
        if (allConflicts.isEmpty()) {
            if (debug.get())
                LOG.debug(String.format("No ConflictPairs between %s<->%s", dtxn, ts));
            return (false);
        }
        
        
        // Now with ConflictPairs, we need to get the minimum set of ProcParameters that are
        // used as input to the conflicting Statements and then check whether they have the same
        // value. If they do, then we cannot run the candidate txn.
        // If one of the ConflictPairs is marked as "Always Conflicting", then we need to stop
        // right away because we simply cannot run the candidate txn
        
        
        return (true);
    }
    
    private boolean analyzeConflicts(AbstractTransaction ts0, AbstractTransaction ts1, Collection<ConflictPair> allConflicts) {
        Procedure proc0 = this.catalogContext.getProcedureById(ts0.getProcedureId());
        ParameterSet params0 = ts0.getProcedureParameters();
        if (params0 == null) {
            if (debug.get())
                LOG.debug(String.format("No Procedure ParameterSet is available for dtxn %s", ts0));
            return (false);
        }
        
        Procedure proc1 = this.catalogContext.getProcedureById(ts1.getProcedureId());
        ParameterSet params1 = ts1.getProcedureParameters();
        assert(params1 != null);
        
        for (ConflictPair cp : allConflicts) {
            ConflictParameterPair pair = this.conflictParams.get(cp);
        }
        
        return (true);
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
    

    private ConflictParameterPair getConflictParameterPair(Statement stmt0, int stmtCtr0, Statement stmt1, int stmtCtr1) {
        int params[][] = new int[2][2];
        for (int i = 0; i < 2; i++) {
            Statement stmt = (i == 0 ? stmt0 : stmt1);
            int stmtCtr = (i == 0 ? stmtCtr0 : stmtCtr1);
            for (Table tbl : this.stmtTablesCache.get(stmt)) {
                for (Column col : this.pkeysCache.get(tbl)) {
                    
                }
            }
            
            // Get the ParameterMappings. We only need to examine the
            // ones for the primary key columns of the table being
            // referenced in the Statement
            Map<StmtParameter, SortedSet<ParameterMapping>> mappings = this.catalogContext.paramMappings.get(stmt, stmtCtr);
            
        }
        
        return (null);
    }

}