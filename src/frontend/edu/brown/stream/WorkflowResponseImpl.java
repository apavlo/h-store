/**
 * @author Wang Hao <wanghao.buaa@gmail.com>
 */
package edu.brown.stream;

import java.io.IOException;

import org.voltdb.client.ClientResponse;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializable;
import org.voltdb.messaging.FastSerializer;

import edu.brown.hstore.Hstoreservice.Status;

public class WorkflowResponseImpl implements WorkflowResponse, FastSerializable {

    private long initiateTime = -1;
    private long endTime = -1;
    Status status = Status.OK;
    
    public WorkflowResponseImpl() {
        // TODO Auto-generated constructor stub
    }

    @Override
    public boolean isInitialized() {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public void finish() {
        // TODO Auto-generated method stub

    }

    @Override
    public void readExternal(FastDeserializer in) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public void writeExternal(FastSerializer out) throws IOException {
        // TODO Auto-generated method stub

    }

    @Override
    public int getClusterRoundtrip() {
        // TODO Auto-generated method stub
        return (int)(endTime-initiateTime);
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public void addClientResponse(ClientResponse cresponse) {
//        if(initiateTime==-1)
//            initiateTime = cresponse.getInitiateTime();
//        else
//            initiateTime = Math.min(cresponse.getInitiateTime(),initiateTime);
//        endTime = Math.max(cresponse.getInitiateTime()+(long)cresponse.getClusterRoundtrip(),endTime);
//        
//        if (cresponse.getStatus() != Status.OK)
//        {
//            // FIXME: we should maintain a map for all procedures, not just a value
//            status = cresponse.getStatus();
//        }            
    }

}
