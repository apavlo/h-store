package edu.brown.hstore.estimators.remote;

import java.util.ArrayList;
import java.util.List;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.Statement;
import org.voltdb.utils.NotImplementedException;

import edu.brown.catalog.special.CountedStatement;
import edu.brown.hstore.Hstoreservice.QueryEstimate;
import edu.brown.hstore.estimators.TransactionEstimate;
import edu.brown.markov.EstimationThresholds;
import edu.brown.utils.PartitionSet;

/**
 * A RemoteEstimate is a partial TransactionEstimate for a txn that
 * is running at remote node. Not all of the internal probabilities
 * will be available for it.
 * @author pavlo
 */
public class RemoteEstimate implements TransactionEstimate {
    
    private final QueryEstimate query_estimates[];
    private final List<CountedStatement> countedStmts[];
    private final CatalogContext catalogContext;
    
    @SuppressWarnings("unchecked")
    public RemoteEstimate(CatalogContext catalogContext) {
        this.catalogContext = catalogContext;
        this.query_estimates = new QueryEstimate[this.catalogContext.numberOfPartitions];
        this.countedStmts = (List<CountedStatement>[])new List<?>[this.catalogContext.numberOfPartitions];
    }

    @Override
    public boolean isInitialized() {
        return (true);
    }

    @Override
    public void finish() {
        for (int p = 0; p < this.query_estimates.length; p++) {
            this.query_estimates[p] = null;
            if (this.countedStmts[p] != null) this.countedStmts[p].clear(); 
        } // FOR
    }

    @Override
    public boolean isValid() {
        return (true);
    }
    
    @Override
    public boolean hasQueryEstimate(int partition) {
        return (this.query_estimates[partition] != null);
    }
    
    public void addQueryEstimate(QueryEstimate est, int partition) {
        this.query_estimates[partition] = est;
    }

    @Override
    public List<CountedStatement> getQueryEstimate(int partition) {
        if (this.countedStmts[partition] == null) {
            this.countedStmts[partition] = new ArrayList<CountedStatement>();
        }
        
        QueryEstimate est = this.query_estimates[partition];
        for (int i = 0, cnt = est.getStmtIdsCount(); i < cnt; i++) {
            Statement catalog_stmt = this.catalogContext.getStatementById(est.getStmtIds(i));
            assert(catalog_stmt != null) : "Invalid Statement id '" + est.getStmtIds(i) + "'";
            this.countedStmts[partition].add(new CountedStatement(catalog_stmt, est.getStmtIds(i)));
        } // FOR
        return (this.countedStmts[partition]);
    }

    @Override
    public PartitionSet getTouchedPartitions(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isSinglePartitionProbabilitySet() {
        return (false);
    }

    @Override
    public boolean isSinglePartitioned(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isReadOnlyProbabilitySet(int partition) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isReadOnlyPartition(EstimationThresholds t, int partition) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isReadOnlyAllPartitions(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public PartitionSet getReadOnlyPartitions(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isWriteProbabilitySet(int partition) {
        return (false);
    }

    @Override
    public boolean isWritePartition(EstimationThresholds t, int partition) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public PartitionSet getWritePartitions(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isFinishProbabilitySet(int partition) {
        return (false);
    }

    @Override
    public boolean isFinishPartition(EstimationThresholds t, int partition) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public PartitionSet getFinishPartitions(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }

    @Override
    public boolean isAbortProbabilitySet() {
        return false;
    }

    @Override
    public boolean isAbortable(EstimationThresholds t) {
        throw new NotImplementedException(this.getClass().getSimpleName() + " does not implement this method");
    }
}
