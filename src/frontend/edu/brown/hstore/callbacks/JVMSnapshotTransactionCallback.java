package edu.brown.hstore.callbacks;

import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import org.voltdb.messaging.FastDeserializer;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Jvmsnapshot.TransactionResponse;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;


public class JVMSnapshotTransactionCallback implements RpcCallback<TransactionResponse> {
    private static final Logger LOG = Logger.getLogger(JVMSnapshotTransactionCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    private HStoreSite hstore_site;
    private LocalTransaction ts;
    
	public JVMSnapshotTransactionCallback(HStoreSite hstore_site, LocalTransaction ts) {
		this.hstore_site = hstore_site;
		this.ts = ts;
	}

	@Override
	public void run(TransactionResponse parameter) {
		// TODO Auto-generated method stub
		if (debug.val) LOG.debug("Received callback from the snapshot");
		FastDeserializer in = new FastDeserializer(parameter.getOutput().toByteArray());
		ClientResponseImpl response = new ClientResponseImpl();
		try {
			response.readExternal(in);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (debug.val) LOG.debug("Msg: "+response.toString());
		
		this.hstore_site.responseSend(ts, response);
		
		this.hstore_site.getJvmSnapshotManager().notify();
		
	}

}
