package edu.brown.benchmark.smallbank.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * DepositChecking Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class DepositChecking extends VoltProcedure {
    
    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated 
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(long acctId, double amount) {
        voltQueueSQL(GetAccount, acctId);
        VoltTable results[] = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = "Invalid account name '" + acctId + "'";
            throw new VoltAbortException(msg);
        }
        // long acctId = results[0].asScalarLong();
        
        voltQueueSQL(UpdateCheckingBalance, amount, acctId);
        results = voltExecuteSQL(true);
        
        // TODO: Do we need to check whether we actually updated something?
        
        return (results[0]);
    }
}