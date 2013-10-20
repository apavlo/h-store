package edu.brown.hstore.callbacks;

import org.apache.log4j.Logger;
import org.voltdb.ClientResponseImpl;
import com.google.protobuf.RpcCallback;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Jvmsnapshot.TransactionResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;


public class JVMSnapshotTransactionCallback implements RpcCallback<TransactionResponse> {
    private static final Logger LOG = Logger.getLogger(JVMSnapshotTransactionCallback.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private RpcCallback<ClientResponseImpl> clientCallback;
    private long client_handle;
    
	public JVMSnapshotTransactionCallback(
			long client_handle, RpcCallback<ClientResponseImpl> clientCallback) {
		// TODO Auto-generated constructor stub
		this.clientCallback = clientCallback;
		this.client_handle = client_handle;
	}

	@Override
	public void run(TransactionResponse parameter) {
		// TODO Auto-generated method stub
		LOG.info("Received callback from the snapshot");
		String msg = parameter.getOutput().toString();
		LOG.info("Msg: "+msg);
		ClientResponseImpl response = new ClientResponseImpl(
												client_handle,
												-1,
												-1,
												Status.OK,
												HStoreConstants.EMPTY_RESULT,
												"JVM succeed!");
		clientCallback.run(response);
	}

}
