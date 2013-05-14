package edu.brown.hstore.txns;

import org.voltdb.catalog.Procedure;

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

}
