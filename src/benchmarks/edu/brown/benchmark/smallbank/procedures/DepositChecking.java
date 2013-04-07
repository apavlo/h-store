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
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT custid FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE name = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(String acctName, double amount) {
        voltQueueSQL(GetAccount, acctName);
        VoltTable results[] = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = "Invalid account name '" + acctName + "'";
            throw new VoltAbortException(msg);
        }
        
        long acctId = results[0].asScalarLong();
        voltQueueSQL(UpdateCheckingBalance, acctId, amount);
        results = voltExecuteSQL(true);
        
        // TODO: Do we need to check whether we actually updated something?
        
        return (results[0]);
    }
}