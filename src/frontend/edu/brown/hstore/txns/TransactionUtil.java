package edu.brown.hstore.txns;

import org.voltdb.catalog.Procedure;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;

import edu.brown.hstore.Hstoreservice.TransactionInitRequest;

/**
 * Simple utility methods for transactions
 */
public abstract class TransactionUtil {

    protected static String debugStmtDep(int stmt_counter, int dep_id) {
        return String.format("{StmtCounter:%d, DependencyId:%d}", stmt_counter, dep_id);
    }

    protected static String debugPartDep(int partition, int dep_id) {
        return String.format("{Partition:%d, DependencyId:%d}", partition, dep_id);
    }
    
    protected static String debugStmtFrag(int stmtCounter, int fragment_id) {
        return String.format("{StmtCounter:%d, FragmentId:%d}", stmtCounter, fragment_id);
    }

    public static String formatTxnName(Procedure catalog_proc, Long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }

    /**
     * Create a TransactionInitRequest builder for the given txn.
     * If paramsSerializer is not null, we will include the procedure ParameterSet
     * in the builder's message.
     * @param ts
     * @param paramsSerializer
     * @return
     */
    public static TransactionInitRequest.Builder createTransactionInitBuilder(LocalTransaction ts, FastSerializer paramsSerializer) {
        TransactionInitRequest.Builder builder = TransactionInitRequest.newBuilder()
                                                        .setTransactionId(ts.getTransactionId().longValue())
                                                        .setProcedureId(ts.getProcedure().getId())
                                                        .setBasePartition(ts.getBasePartition())
                                                        .addAllPartitions(ts.getPredictTouchedPartitions());
        if (paramsSerializer != null) {
            FastSerializer fs = paramsSerializer;
            try {
                fs.clear();
                ts.getProcedureParameters().writeExternal(fs);
                builder.setProcParams(ByteString.copyFrom(fs.getBBContainer().b));
            } catch (Exception ex) {
                throw new RuntimeException("Failed to serialize ParameterSet for " + ts, ex);
            }
        }
        
        return (builder);
    }
    
}
