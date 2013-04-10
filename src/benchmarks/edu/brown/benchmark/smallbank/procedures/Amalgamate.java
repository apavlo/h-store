package edu.brown.benchmark.smallbank.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * Amalgamate Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class Amalgamate extends VoltProcedure {
    
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
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(String acctName0, String acctName1, double amount) {
        // Get Account Information
        voltQueueSQL(GetAccount, acctName0);
        voltQueueSQL(GetAccount, acctName1);
        final VoltTable acctResults[] = voltExecuteSQL();
        if (acctResults[0].getRowCount() != 1) {
            String msg = "Invalid account name '" + acctName0 + "'";
            throw new VoltAbortException(msg);
        }
        if (acctResults[1].getRowCount() != 1) {
            String msg = "Invalid account name '" + acctName1 + "'";
            throw new VoltAbortException(msg);
        }
        long acctId0 = acctResults[0].asScalarLong();
        long acctId1 = acctResults[1].asScalarLong();
        
        // Get Balance Information
        voltQueueSQL(GetSavingsBalance, acctId0);
        voltQueueSQL(GetCheckingBalance, acctId0);
        final VoltTable balResults[] = voltExecuteSQL();
        if (balResults[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId0);
            throw new VoltAbortException(msg);
        }
        if (balResults[1].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, 
                                       acctId0);
            throw new VoltAbortException(msg);
        }
        balResults[0].advanceRow();
        balResults[1].advanceRow();
        double total = balResults[0].getDouble(0) + balResults[1].getDouble(0);  

        // Update Balance Information
        voltQueueSQL(UpdateSavingsBalance, acctId0, 0.0);
        voltQueueSQL(UpdateCheckingBalance, acctId0, 0.0);
        
                
//        if (balance < 0) {
//            String msg = String.format("Negative %s balance for customer #%d",
//                                       SmallBankConstants.TABLENAME_SAVINGS, 
//                                       acctId);
//            throw new VoltAbortException(msg);
//        }
//        
//        voltQueueSQL(UpdateSavingsBalance, amount);
//        results = voltExecuteSQL(true);
//        return (results[0]);
        return (null);
    }
}