package edu.brown.hstore.internal;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;

/**
 * A class to hold information about deferred queries so they can be queued an dequed when needed.
 * @param txid The transaction ID of the transaction from which the deferred query came, to be re-used when it runs
 * @param stmt The SQLStmt
 * @param paramMappings The parameters the SQLStmt was called with
 *
 */
public class DeferredQueryMessage extends InternalMessage {
    private Long _txnid;
    private SQLStmt _stmt;
    private ParameterSet _params;
    
    public DeferredQueryMessage(Long txnid, SQLStmt stmt, ParameterSet params){
        // TODO: have it take a timer, also! so we can see how long deferred work lasts, on average
        _txnid = txnid;
        _stmt = stmt;
        _params = params;
    }
    
    public Long getTxnId(){
        return _txnid;
    }
    public SQLStmt getStmt(){
        return _stmt;
    }
    public ParameterSet getParams(){
        return _params;
    }
}
