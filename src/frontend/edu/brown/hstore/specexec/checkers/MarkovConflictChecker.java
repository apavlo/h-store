package edu.brown.hstore.specexec.checkers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
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
import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.estimators.Estimate;
import edu.brown.hstore.estimators.EstimatorState;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.mappings.ParameterMapping;
import edu.brown.mappings.ParameterMappingsSet;
import edu.brown.mappings.ParametersUtil;
import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.StringUtil;

/**
 * A more fine-grained ConflictChecker based on estimations of what 
 * rows the txns will read/write.
 * @author pavlo
 */
public class MarkovConflictChecker extends TableConflictChecker {
    private static final Logger LOG = Logger.getLogger(MarkovConflictChecker.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final ParameterMappingsSet paramMappings;
    @SuppressWarnings("unused")
    private EstimationThresholds thresholds;

    // ----------------------------------------------------------------------------
    // PRE-COMPUTED CACHE
    // ----------------------------------------------------------------------------

    protected static class StatementCache {
        /**
         * We maintain a mapping to another Statement that it conflicts
         * with and its corresponding ConflictPair that represents that conflict
         */
        final Map<Statement, ConflictPair> conflicts = new HashMap<Statement, ConflictPair>();  

        /**
         * We maintain a list of the StmtParameters that are used in predicates with 
         * the target table's primary key
         */
        final Map<Column, StmtParameter> colParams = new IdentityHashMap<Column, StmtParameter>();
    } // CLASS
    
    /**
     * We have a separate cache object for each Statement
     */
    protected final Map<Statement, StatementCache> stmtCache = new HashMap<Statement, StatementCache>();
    
    /**
     * Table -> Primary Keys
     */
    protected final Map<Table, Column[]> pkeysCache = new HashMap<Table, Column[]>();
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Constructor
     * @param catalogContext
     * @param thresholds
     */
    protected MarkovConflictChecker(CatalogContext catalogContext, EstimationThresholds thresholds) {
        super(catalogContext);
        this.paramMappings = catalogContext.paramMappings;
        this.thresholds = thresholds;
        
        if (this.paramMappings == null) {
            LOG.warn(String.format("Disabling %s because the %s in the %s is null",
                     this.getClass().getSimpleName(),
                     ParameterMappingsSet.class.getSimpleName(),
                     catalogContext.getClass().getSimpleName()));
            this.disabled = true;
        }
        
        for (Table catalog_tbl : CatalogUtil.getDataTables(this.catalogContext.database)) {
            this.pkeysCache.put(catalog_tbl, CatalogUtil.getPrimaryKeyColumns(catalog_tbl).toArray(new Column[0]));
        } // FOR (table)
        
        for (Procedure proc : this.catalogContext.getRegularProcedures()) {
            Collection<ConflictPair> conflicts = ConflictSetUtil.getAllConflictPairs(proc);
            for (Statement stmt : proc.getStatements()) {
                StatementCache cache = new StatementCache();
                for (ConflictPair cp : conflicts) {
                    if (cp.getStatement0().equals(stmt)) {
                        cache.conflicts.put(cp.getStatement1(), cp);
                    }
                } // FOR
                
                PredicatePairs cset = CatalogUtil.extractStatementPredicates(stmt, false);
                Map<Table, StmtParameter[]> tableParams = new HashMap<Table, StmtParameter[]>();
                List<StmtParameter> stmtParamOffsets = new ArrayList<StmtParameter>();
                for (Table tbl : CatalogUtil.getReferencedTables(stmt)) {
                    Column pkeys[] = this.pkeysCache.get(tbl);
                    if (trace.val) LOG.trace(tbl + " => " + Arrays.toString(pkeys));
                    if (pkeys == null) {
                        LOG.warn("Unexpected null primary keys for " + tbl);
                        continue;
                    }
                    for (Column col : pkeys) {
                        Collection<StmtParameter> params = cset.findAllForOther(StmtParameter.class, col);
                        // If there are more than one, then it should always conflict
                        if (params.size() > 1) {
                            // TODO
                            LOG.warn(String.format("There are %d %s mapped to the primary key column %s. " +
                                     "Marking %s as always conflicting with %s",
                                     params.size(), StmtParameter.class.getSimpleName(), col.fullName(),
                                     tbl, stmt.fullName()));
                        }
                        else {
                            // If there are no references, then there is nothing else that we 
                            // we need to do here. The StmtParameter that we get back will be null
                            StmtParameter stmtParam = CollectionUtil.first(params);
                            cache.colParams.put(col, stmtParam);
                        }
                    } // FOR
                    tableParams.put(tbl, stmtParamOffsets.toArray(new StmtParameter[0]));
                } // FOR
                this.stmtCache.put(stmt, cache);
            } // FOR (stmt)
        } // FOR (proc)
    }

    @Override
    public void setEstimationThresholds(EstimationThresholds t) {
        this.thresholds = t;
    }
    
    @Override
    public boolean shouldIgnoreTransaction(AbstractTransaction ts) {
        if (this.disabled) return (true);

        // NOTE: We actually don't want to check whether we have a transaction estimate here
        // because this is a global flag. There may be times when the txn doesn't 
        // actually need an estimate (e.g., when we're stalled because of 2PC), so we 
        // just want to always return false here!
        
        // We're good to go!
        return (false);
    }
    
    @Override
    public boolean skipConflictAfter() {
        return (true);
    }
    
    @Override
    public boolean hasConflictBefore(AbstractTransaction dtxn, LocalTransaction candidate, int partitionId) {
        // If the TableConflictChecker says that there is no conflict, then we know that 
        // we don't need to check anything else.
        if (super.hasConflictBefore(dtxn, candidate, partitionId) == false) {
            if (debug.val)
                LOG.debug(String.format("No table-level conflicts between %s and %s. Safe to execute!",
                          dtxn, candidate));
            return (false);
        }
        
        // Get the estimates for both of the txns
        // If we don't have an estimate, then we have to say that there is a conflict. 
        EstimatorState dtxnState = dtxn.getEstimatorState();
        EstimatorState tsState = candidate.getEstimatorState();
        if (dtxnState == null) {
            if (debug.val)
                LOG.debug(String.format("No %s available for distributed txn %s",
                          EstimatorState.class.getSimpleName(), dtxn));
            return (true);
        }
        else if (tsState == null) {
            if (debug.val)
                LOG.debug(String.format("No %s available for candidate txn %s",
                          EstimatorState.class.getSimpleName(), candidate));
            return (true);
        }
        
        // Get the current TransactionEstimate for the DTXN and the 
        // initial TransactionEstimate for the single-partition txn
        // We need to make sure that both estimates have the list of 
        // queries that the transaction is going to execute
        Estimate dtxnEst = dtxnState.getLastEstimate();
        if (dtxnEst == null) {
            if (debug.val)
                LOG.warn(String.format("Unexpected null %s in the %s for %s",
                         Estimate.class.getSimpleName(), dtxnState.getClass().getSimpleName(), dtxn));
            return (true);
        }
        else if (dtxnEst.hasQueryEstimate(partitionId) == false) {
            if (debug.val)
                LOG.warn(String.format("No query list estimate is available for dtxn %s", dtxn));
            return (true);
        }
        Estimate tsEst = tsState.getInitialEstimate();
        assert(tsEst != null);
        if (tsEst.hasQueryEstimate(partitionId) == false) {
            if (debug.val)
                LOG.warn(String.format("No query list estimate is available for candidate %s", candidate));
            return (true);
        }
        
        // If both txns are read-only, then we can let our homeboy go
//        boolean readonly0 = dtxnEst.isReadOnlyPartition(this.thresholds, partitionId);
//        boolean readonly1 = tsEst.isReadOnlyPartition(this.thresholds, partitionId);
//        if (readonly0 && readonly1) {
//            if (debug.val)
//                LOG.debug(String.format("%s<->%s are both are read-only. No conflict!", dtxn, ts));
//            return (false);
//        }
        
        List<CountedStatement> queries0 = dtxnEst.getQueryEstimate(partitionId);
        List<CountedStatement> queries1 = tsEst.getQueryEstimate(partitionId);
        
        return (this.canExecute(dtxn, queries0, candidate, queries1) == false);
    }
    
    /**
     * Internal method that checks whether the txns conflict based on their list
     * queries that they're expected to execute in the future.
     * @param ts0
     * @param queries0
     * @param ts1
     * @param queries1
     * @return
     */
    protected boolean canExecute(AbstractTransaction ts0, List<CountedStatement> queries0,
                                 AbstractTransaction ts1, List<CountedStatement> queries1) {
        ParameterSet params0 = ts0.getProcedureParameters();
        ParameterSet params1 = ts1.getProcedureParameters();
        CountedStatement stmt0, stmt1;
        StatementCache cache0, cache1;
        Map<StmtParameter, SortedSet<ParameterMapping>> mappings0, mappings1;
        
        if (params0 == null) {
            LOG.error(String.format("The ParameterSet for %s is null.", ts0));
            return (false);
        }
        else if (params1 == null) {
            LOG.error(String.format("The ParameterSet for %s is null.", ts1));
            return (false);
        }
        
        if (debug.val)
            LOG.debug(String.format("Comparing expected query execution for dtxn %s [#queries=%d] " +
            		  "with candidate txn %s [#queries=%d]",
            		  ts0, queries0.size(), ts1, queries1.size()));
        
        // TODO: Rather than checking the values referenced in each ConflictPair
        // individually, we should go through all of them first and just get the 
        // ProcParameter + Offsets that actually matter. Then we can just check the
        // values one after each other.
        
        // Now with ConflictPairs, we need to get the minimum set of ProcParameters that are
        // used as input to the conflicting Statements and then check whether they have the same
        // value. If they do, then we cannot run the candidate txn.
        
        // (1) We only need to examine the READ-WRITE conflicts and WRITE->WRITE
        
        for (int i0 = 0, cnt0 = queries0.size(); i0 < cnt0; i0++) {
            stmt0 = queries0.get(i0);
            cache0 = this.stmtCache.get(stmt0.statement);
            mappings0 = this.catalogContext.paramMappings.get(stmt0.statement, stmt0.counter);
            if (mappings0 == null) {
                LOG.warn(String.format("The ParameterMappings for %s in dtxn %s is null?\n%s",
                         stmt0, ts0, StringUtil.join("\n", queries0)));
                return (false);
            }
            
            for (int i1 = 0, cnt1 = queries1.size(); i1 < cnt1; i1++) {
                stmt1 = queries1.get(i1);
                if (stmt0.statement.getReadonly() && stmt1.statement.getReadonly()) continue;
                
                ConflictPair cp = cache0.conflicts.get(stmt1.statement);
                // If there isn't a ConflictPair, then there isn't a conflict
                if (cp == null) {
                    continue;
                }
                // If the ConflictPair is marked as always conflicting, then
                // we can stop right here
                else if (cp.getAlwaysconflicting()) {
                    if (debug.val) LOG.debug(String.format("%s - Marked as always conflicting", cp.fullName()));
                    return (false);
                }
                
                // Otherwise, at this point we know that we have two queries that both 
                // reference the same table(s). Therefore, we need to evaluate the values 
                // of the primary keys referenced in the queries to see whether they conflict
                cache1 = this.stmtCache.get(stmt1.statement);
                mappings1 = this.catalogContext.paramMappings.get(stmt1.statement, stmt1.counter);
                if (mappings1 == null) {
                    LOG.warn(String.format("The ParameterMappings for %s in candidate %s is null?\n%s",
                             stmt1, ts1, StringUtil.join("\n", queries1)));
                    return (false);
                }
                
                boolean allEqual = true;
                for (Column col : cache0.colParams.keySet()) {
                    // If either StmtParameters are null, then that's a conflict!
                    StmtParameter param0 = cache0.colParams.get(col);
                    StmtParameter param1 = cache1.colParams.get(col);
                    
                    // It's ok if we're missing one of the parameters that we need if
                    // the values are still not the same. It's only a problem if they're 
                    // all the same because we have no idea know whether they're 
                    // actually the same or not
                    if (param0 == null || param1 == null) {
                        if (trace.val)
                            LOG.trace(String.format("%s - Missing StmtParameters for %s [param0=%s / param1=%s]",
                                      cp.fullName(), col.fullName(), param0, param1));
                        continue;
                    }
                    
                    // Similarly, if we don't have a ParameterMapping then there is nothing
                    // else we can do. Again, this is ok as long as at least one of the 
                    // pkey values are different.
                    ParameterMapping pm0 = CollectionUtil.first(mappings0.get(param0));
                    ParameterMapping pm1 = CollectionUtil.first(mappings1.get(param1));
                    if (pm0 == null) {
                        if (trace.val)
                            LOG.trace(String.format("%s - No ParameterMapping for %s",
                                      cp.fullName(), param0.fullName()));
                        continue;
                    }
                    else if (pm1 == null) {
                        if (trace.val)
                            LOG.trace(String.format("%s - No ParameterMapping for %s",
                                      cp.fullName(), param1.fullName()));
                        continue;
                    }
                    
                    // If the values are not equal, then we can stop checking the 
                    // other columns right away.
                    if (this.equalParameters(params0, pm0, params1, pm1) == false) {
                        if (trace.val)
                            LOG.trace(String.format("%s - Parameter values are equal for %s [param0=%s / param1=%s]",
                                      cp.fullName(), col.fullName(), param0, param1));
                        allEqual = false;
                        break;
                    }
                } // FOR (col)
                
                // If all the parameters are equal, than means they are likely to be
                // accessing the same row in the table. That's a conflict!
                if (allEqual) {
                    if (debug.val)
                        LOG.debug(String.format("%s - All known parameter values are equal", cp.fullName()));
                    return (false);
                }
            } // FOR (stmt1)
        } // FOR (stmt0)
        return (true);
    }
    
    protected boolean equalParameters(ParameterSet params0, ParameterMapping pm0,
                                      ParameterSet params1, ParameterMapping pm1) {
        Object val0 = ParametersUtil.getValue(params0, pm0);
        Object val1 = ParametersUtil.getValue(params1, pm1);
        if (val0 == null) {
            return (val1 != null);
        } else if (val1 == null) {
            return (false);
        }
        return (val0.equals(val1));
    }
    
    // ----------------------------------------------------------------------------
    // SINGLETON
    // ----------------------------------------------------------------------------
    
    private static MarkovConflictChecker SINGLETON;
    public static MarkovConflictChecker singleton(CatalogContext catalogContext, EstimationThresholds t) {
        if (SINGLETON == null) {
            synchronized (MarkovConflictChecker.class) {
                if (SINGLETON == null) {
                    SINGLETON = new MarkovConflictChecker(catalogContext, t);
                }
            } // SYNCH
        }
        return (SINGLETON);
    }
    
    
}
