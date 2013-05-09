package edu.brown.benchmark.smallbank.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * SendPayment Procedure
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class SendPayment extends VoltProcedure {
    
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetCheckingBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_CHECKING +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateCheckingBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_CHECKING + 
        "   SET bal = bal + ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable[] run(long sendAcct, long destAcct, double amount) {
        // Get Account Information
        voltQueueSQL(GetAccount, sendAcct);
        voltQueueSQL(GetAccount, destAcct);
        final VoltTable acctResults[] = voltExecuteSQL();
        if (acctResults[0].getRowCount() != 1) {
            String msg = "Invalid sender account '" + sendAcct + "'";
            throw new VoltAbortException(msg);
        }
        else if (acctResults[1].getRowCount() != 1) {
            String msg = "Invalid destination account '" + destAcct + "'";
            throw new VoltAbortException(msg);
        }
        
        // Get the sender's account balance
        voltQueueSQL(GetCheckingBalance, sendAcct);
        final VoltTable balResults[] = voltExecuteSQL();
        if (balResults[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       sendAcct);
            throw new VoltAbortException(msg);
        }
        balResults[0].advanceRow();
        double balance = balResults[0].getDouble(0);
        
        if (balance < amount) {
            String msg = String.format("Insufficient %s funds for customer #%d",
                                       SmallBankConstants.TABLENAME_CHECKING, sendAcct); 
            throw new VoltAbortException(msg);
        }
        
        // Debt
        voltQueueSQL(UpdateCheckingBalance, amount*-1d, sendAcct);
        
        // Credit
        voltQueueSQL(UpdateCheckingBalance, amount, destAcct);
        
        return (voltExecuteSQL(true));
    }
}