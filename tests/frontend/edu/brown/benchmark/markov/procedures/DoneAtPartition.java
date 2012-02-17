package edu.brown.benchmark.markov.procedures;

import java.util.HashSet;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.markov.MarkovConstants;

/**
 * DoneProcedure This procedure has a series of multi partition writes and reads
 * followed by a single single-partition write. The MarkovGraph should reflect
 * this.
 */
public class DoneAtPartition extends VoltProcedure {
    public final SQLStmt fromC = new SQLStmt("SELECT C_ID, C_A_ID FROM " + MarkovConstants.TABLENAME_TABLEC + " WHERE C_IATTR01 = ?");
    public final SQLStmt updateB = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEB + " SET B_IATTR01 = B_IATTR01 + ? WHERE B_A_ID = ?");
    public final SQLStmt selectB = new SQLStmt("SELECT B_IATTR01, B_ID, B_A_ID  " + "  FROM " + MarkovConstants.TABLENAME_TABLEB + " WHERE B_A_ID = ? " + " ORDER BY B_IATTR01 DESC LIMIT 1");
    public final SQLStmt updateA = new SQLStmt("UPDATE " + MarkovConstants.TABLENAME_TABLEA + "   SET A_IATTR01 = ?, " + // B_ID
            "       A_IATTR02 = ?  " + // MAX(B_IATTR01)
            " WHERE A_ID = ?");

    public VoltTable[] run(long a_id, long value) {
        voltQueueSQL(fromC, value);
        // (MP) Select from C based on a parameter - should be multi-partition
        // we are comparing with a non-partitioning attribute of C
        //
        VoltTable[] intermediate_results = voltExecuteSQL();

        HashSet<Long> c_a_ids = new HashSet<Long>();
        while (intermediate_results[0].advanceRow()) {
            long c_a_id = intermediate_results[0].getLong("C_A_ID");
            voltQueueSQL(updateB, value, c_a_id);
            // (MP - one shot) Update B based on a partitioning attribute of B
            // however, this update is not necessarily on the same partition as
            // our current
            // a_id, so it will go to other partitions to conduct the update
            c_a_ids.add(c_a_id);
        } // WHILE
        voltExecuteSQL();

        // Because we can't do the distributed aggregate using IN, we have to
        // manually
        // find the max for ourselves
        long maxBValue = 0;
        long maxID = 0;
        for (Long c_a_id : c_a_ids) {
            voltQueueSQL(selectB, c_a_id);
            // (MP) We are selecting from B based on a bunch of different
            // c_a_ids we retrieved
            // from our first call. As a result, this is going to be
            // multi-partition most of the time.
            VoltTable[] b_results = voltExecuteSQL();
            assert (b_results.length == 1);
            assert (b_results[0].getRowCount() > 0);
            assert (b_results[0].advanceRow());

            long bValue = b_results[0].getLong("B_IATTR01");
            maxBValue = Math.max(maxBValue, bValue);
            if (maxBValue == bValue) {
                maxID = b_results[0].getLong("B_ID");
            }
        } // FOR

        voltQueueSQL(updateA, maxID, maxBValue, a_id);
        // (SP) An update of the part of A that is at our 'home' partition
        // We match based on the parameter a_id
        return voltExecuteSQL();
    }
}
