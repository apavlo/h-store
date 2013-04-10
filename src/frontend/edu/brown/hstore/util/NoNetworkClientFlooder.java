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
	private int txnCounter = 0;
	private final AtomicInteger successCounter = new AtomicInteger(0);
	private final AtomicInteger rejectCounter = new AtomicInteger(0);
	
	public int clientId;
	
    final RpcCallback<ClientResponseImpl> callback = new RpcCallback<ClientResponseImpl>() {
        @Override
        public void run(ClientResponseImpl clientResponse) {
            // NOTHING!
        	final Status status = clientResponse.getStatus();
        	long rej = 1, suc = 1;
        	if (status == Status.ABORT_REJECT) {
        		rej = rejectCounter.incrementAndGet();
        		if (rej % 100000 == 0 ) {
        			System.out.println(String.format("Client Flooder[%d]:Rejected txns counter:%d", 
        					clientResponse.getClientHandle(), rej));
            	}
        	}
        	if (status == Status.OK) {
        		suc = successCounter.getAndIncrement();
        		if (suc % 100000 == 0 ) {
        			System.out.println(String.format("Client Flooder[%d]:Finished txns counter:%d",
        					clientResponse.getClientHandle(), suc));
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
    	
        while (true) {
            // TODO: Get a PhoneCallGenerator.PhoneCall object!
        	
            this.hstore_site.invocationProcess(buffer, this.callback);
            
            if ( this.txnCounter++ % 100000 == 0) {
        		System.out.println(String.format("Client Flooder[%d]:Total txns created:%d", this.clientId, this.txnCounter));
        	}
            // TODO: Sleep for a little...
            try {
//				Thread.sleep(0, 100000);
//				Thread.sleep(0, 1);
				Thread.sleep(1L);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
        } // WHILE
    }
    
}
