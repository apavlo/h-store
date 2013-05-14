package edu.brown.hstore.specexec;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;
import org.voltdb.catalog.Statement;

import edu.brown.hstore.BatchPlanner.BatchPlan;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.PrefetchState;
import edu.brown.hstore.txns.QueryInvocation;
import edu.brown.utils.PartitionSet;

public abstract class PrefetchQueryUtil {

    public static void checkSQLStmtBatch(PartitionExecutor executor,
                                         LocalTransaction ts,
                                         BatchPlan plan,
                                         int batchSize,
                                         SQLStmt batchStmts[],
                                         ParameterSet batchParams[]) {
        assert(executor.getPartitionId() == ts.getBasePartition());
        
        PartitionSet stmtPartitions[] = plan.getStatementPartitions();
        PrefetchState prefetchState = ts.getPrefetchState();
        QueryTracker queryTracker = prefetchState.getExecQueryTracker();
        QueryTracker prefetchTracker = prefetchState.getPrefetchQueryTracker();
        assert(prefetchState != null);
        for (int i = 0; i < batchSize; i++) {
            Statement stmt = batchStmts[i].getStatement();
            
            // We always have to update the query tracker regardless of whether
            // the query was prefetched or not. This is so that we can ensure
            // that we execute the queries in the right order.
            int stmtCnt = queryTracker.addQuery(stmt, stmtPartitions[i], batchParams[i]);
            
            // But we only need to check for conflicts for queries that were prefetched
            if (stmt.getPrefetchable() == false) continue;
            
            // Check whether we have already sent a prefetch request to execute this query
            QueryInvocation pq = prefetchTracker.findPrefetchedQuery(stmt, stmtCnt);
            if (pq != null) {
                // We have... so that means that we don't want to send it
                // again and should expect the result to come from somewhere else.
                // But we need to check whether we sent it to the same partitions
                boolean samePartitions = pq.partitions.containsAll(stmtPartitions[i]);
                boolean sameParams = (pq.paramsHash == batchParams[i].hashCode());
                
                // Everything is the same, so we need to remove this
                // statement from the batch.
                if (samePartitions && sameParams) {
                    plan.markStatementAsAlreadyPrefetched(i);
                }
                // If it's a read-only query, then we don't care 
                // about it 
                
                // The parameters are different
                else if (sameParams == false) {
                    // If it's a read
                    
                    // TODO
                }
                // The partitions are different
                else if (samePartitions == false) {
                    // TODO
                }
            }
        } // FOR
        
    }
    
}
