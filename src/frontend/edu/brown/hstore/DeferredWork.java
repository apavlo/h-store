package edu.brown.hstore;

import org.voltdb.ParameterSet;
import org.voltdb.SQLStmt;

public class DeferredWork {
	private Long _txid;
	private SQLStmt _stmt;
	private ParameterSet _params;
	
	public DeferredWork(Long txid, SQLStmt stmt, ParameterSet params){
		// TODO: have it take a timer, also! so we can see how long deferred work lasts, on average
		_txid = txid;
		_stmt = stmt;
		_params = params;
	}
	
	public Long getTxId(){
		return _txid;
	}
	public SQLStmt getStmt(){
		return _stmt;
	}
	public ParameterSet getParams(){
		return _params;
	}
}
