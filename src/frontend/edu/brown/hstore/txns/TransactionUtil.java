package edu.brown.hstore.txns;

import org.voltdb.catalog.Procedure;

public abstract class TransactionUtil {

    protected static String debugStmtDep(int stmt_index, int dep_id) {
        return String.format("{StmtIndex:%d, DependencyId:%d}", stmt_index, dep_id);
    }

    protected static String debugPartDep(int partition, int dep_id) {
        return String.format("{Partition:%d, DependencyId:%d}", partition, dep_id);
    }

    public static String formatTxnName(Procedure catalog_proc, Long txn_id) {
        if (catalog_proc != null) {
            return (catalog_proc.getName() + " #" + txn_id);
        }
        return ("#" + txn_id);
    }

}
