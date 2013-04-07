package edu.brown.benchmark.smallbank.procedures;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.smallbank.SmallBankConstants;

@ProcInfo (
    partitionParam=0
)
public class Balance extends VoltProcedure {
    
    private static final VoltTable.ColumnInfo[] RESULT_COLS = {
        new VoltTable.ColumnInfo("TOTAL", VoltType.FLOAT),
    };
	
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
	
    public VoltTable run(String acctName) {
        voltQueueSQL(GetAccount, acctName);
        VoltTable results[] = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = "Invalid account name '" + acctName + "'";
            throw new VoltAbortException(msg);
        }
        
        long acctId = results[0].asScalarLong();
        
        voltQueueSQL(GetSavingsBalance, acctId);
        voltQueueSQL(GetCheckingBalance, acctId);
        results = voltExecuteSQL(true);
        
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
        double total = results[0].getDouble(0) + results[0].getDouble(1); 
        
        final VoltTable finalResult = new VoltTable(RESULT_COLS);
        finalResult.addRow(total);
        return (finalResult);
    }
}