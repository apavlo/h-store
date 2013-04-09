package edu.brown.benchmark.smallbank.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * WriteCheck Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class WriteCheck extends VoltProcedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT custid FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE name = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal - ? " +
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
        voltQueueSQL(GetSavingsBalance, acctId);
        voltQueueSQL(GetCheckingBalance, acctId);
        results = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId);
            throw new VoltAbortException(msg);
        }
        if (results[1].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       acctId);
            throw new VoltAbortException(msg);
        }
        results[0].advanceRow();
        results[1].advanceRow();
        double total = results[0].getDouble(0) + results[1].getDouble(0);
        
        if (total < amount) {
            voltQueueSQL(UpdateCheckingBalance, amount - 1);
        } else {
            voltQueueSQL(UpdateCheckingBalance, amount);
        }
        results = voltExecuteSQL(true);
        return (results[0]);
    }
}