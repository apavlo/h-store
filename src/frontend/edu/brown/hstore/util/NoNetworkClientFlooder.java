package edu.brown.hstore.util;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.voltdb.ClientResponseImpl;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.RpcCallback;
import edu.brown.hstore.HStoreSite;

public class NoNetworkClientFlooder implements Runnable {
    
    final RpcCallback<ClientResponseImpl> callback = new RpcCallback<ClientResponseImpl>() {
        @Override
        public void run(ClientResponseImpl parameter) {
            // NOTHING!
        }
    };
    
    final HStoreSite hstore_site;
    
    public NoNetworkClientFlooder(HStoreSite hstore_site) {
        this.hstore_site = hstore_site;
    }
    
    @Override
    public void run() {
    	PhoneCallGenerator pcg = new PhoneCallGenerator(1000+1, 4);
    	
        while (true) {
            // TODO: Get a PhoneCallGenerator.PhoneCall object!
        	PhoneCallGenerator.PhoneCall call = pcg.receive();
            StoredProcedureInvocation spi = new StoredProcedureInvocation(
            		1000+1, 
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
            
            // TODO: Sleep for a little...
//            try {
//				Thread.sleep(1);
//			} catch (InterruptedException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
        } // WHILE
    }
    
}
