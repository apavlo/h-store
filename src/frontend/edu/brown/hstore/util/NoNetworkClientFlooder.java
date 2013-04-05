package edu.brown.hstore.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.RpcCallback;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.Status;

public class NoNetworkClientFlooder implements Runnable {
	private final AtomicInteger txnCounter = new AtomicInteger(0);
	private final AtomicInteger successCounter = new AtomicInteger(0);
	private final AtomicInteger rejectCounter = new AtomicInteger(0);
	
	private int clientId;
	
    final RpcCallback<ClientResponseImpl> callback = new RpcCallback<ClientResponseImpl>() {
        @Override
        public void run(ClientResponseImpl clientResponse) {
            // NOTHING!
        	final Status status = clientResponse.getStatus();
        	long rej = 1, suc = 1;
        	if (status == Status.ABORT_REJECT) {
        		rej = rejectCounter.incrementAndGet();
        		if (rej % 1000 == 0 ) {
            		System.out.println("Rejected txns counter: " + rej);
            	}
        	}
        	if (status == Status.OK) {
        		suc = successCounter.getAndIncrement();
        		if (suc % 1000 == 0 ) {
            		System.out.println("Finished txns counter: " + suc );
            	}
        	}
        	
        }
    };
    
    final HStoreSite hstore_site;
    
    public NoNetworkClientFlooder(HStoreSite hstore_site, int clientId) {
        this.hstore_site = hstore_site;
        this.clientId = clientId;
    }
    
    @Override
    public void run() {
    	PhoneCallGenerator pcg = new PhoneCallGenerator(this.clientId, 4);
    	
        while (true) {
            // TODO: Get a PhoneCallGenerator.PhoneCall object!
        	PhoneCallGenerator.PhoneCall call = pcg.receive();
            StoredProcedureInvocation spi = new StoredProcedureInvocation(
            		this.clientId, 
            		"Vote",
                    call.voteId, 
                    call.phoneNumber,
                    call.contestantNumber,
                    1000);
            ByteBuffer buffer = null;
			try {
				buffer = ByteBuffer.wrap(FastSerializer.serialize(spi));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
            
            this.hstore_site.invocationProcess(buffer, this.callback);
            
            long ait = this.txnCounter.incrementAndGet();
            if ( ait % 1000 == 0) {
        		System.out.println("txns total counter: " + txnCounter.longValue() );
        	}
            // TODO: Sleep for a little...
//            try {
//				Thread.sleep(0, 1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
        } // WHILE
    }
    
}
