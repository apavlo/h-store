package org.voltdb.regressionsuites.specexecprocs;

import org.voltdb.ProcInfo;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.procedures.SendPayment;

/**
 * Special version of SmallBank's SendPayment that is not blockable but
 * will not abort if there is no data.
 * @author pavlo
 */
@ProcInfo(
    partitionParam = 0,
    singlePartition = false
)
public class NonBlockingSendPayment extends SendPayment {
    
    public VoltTable[] run(long sendAcct, long destAcct, double amount) {
        // BATCH #1
        voltQueueSQL(GetAccount, sendAcct);
        voltQueueSQL(GetAccount, destAcct);
        final VoltTable acctResults[] = voltExecuteSQL();
        assert(acctResults != null);
        
        // BATCH #2
        voltQueueSQL(GetCheckingBalance, sendAcct);
        final VoltTable balResults[] = voltExecuteSQL();
        assert(balResults != null);
        
        // BATCH #3
        voltQueueSQL(UpdateCheckingBalance, amount*-1d, sendAcct);
        voltQueueSQL(UpdateCheckingBalance, amount, destAcct);
        final VoltTable updateResults[] = voltExecuteSQL();
        
        return (updateResults);
    }
}
