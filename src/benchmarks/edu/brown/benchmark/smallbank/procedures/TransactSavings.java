package edu.brown.benchmark.smallbank.procedures;

import org.apache.log4j.Logger;
import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;

import edu.brown.benchmark.smallbank.SmallBankConstants;

/**
 * TransactSavings Procedure
 * Original version by Mohammad Alomari and Michael Cahill
 * @author pavlo
 */
@ProcInfo (
    partitionParam=0
)
public class TransactSavings extends VoltProcedure {
    private static final Logger LOG = Logger.getLogger(TransactSavings.class);

    
    // 2013-05-05
    // In the original version of the benchmark, this is suppose to be a look up
    // on the customer's name. We don't have fast implementation of replicated 
    // secondary indexes, so we'll just ignore that part for now.
    public final SQLStmt GetAccount = new SQLStmt(
        "SELECT * FROM " + SmallBankConstants.TABLENAME_ACCOUNTS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt GetSavingsBalance = new SQLStmt(
        "SELECT bal FROM " + SmallBankConstants.TABLENAME_SAVINGS +
        " WHERE custid = ?"
    );
    
    public final SQLStmt UpdateSavingsBalance = new SQLStmt(
        "UPDATE " + SmallBankConstants.TABLENAME_SAVINGS + 
        "   SET bal = bal - ? " +
        " WHERE custid = ?"
    );
    
    public VoltTable run(long acctId, double amount) {
        voltQueueSQL(GetAccount, acctId);
        VoltTable results[] = voltExecuteSQL();
        
        if (results[0].getRowCount() != 1) {
            String msg = "Invalid account '" + acctId + "'";
            LOG.error(msg);
            throw new VoltAbortException(msg);
        }
        // long acctId = results[0].asScalarLong();
        
        voltQueueSQL(GetSavingsBalance, acctId);
        results = voltExecuteSQL();
        if (results[0].getRowCount() != 1) {
            String msg = String.format("No %s for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId);
            LOG.error(msg);
            throw new VoltAbortException(msg);
        }
        
        results[0].advanceRow();
        double balance = results[0].getDouble(0) + amount;
        if (balance < 0) {
            String msg = String.format("Negative %s balance for customer #%d",
                                       SmallBankConstants.TABLENAME_SAVINGS, 
                                       acctId);
            LOG.error(msg);
            throw new VoltAbortException(msg);
        }
        
        voltQueueSQL(UpdateSavingsBalance, amount, acctId);
        results = voltExecuteSQL(true);
        return (results[0]);
    }
}