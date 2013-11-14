/**
 * @author Wang Hao <wanghao.buaa@gmail.com>
 */
package edu.brown.stream;

import org.voltdb.client.ClientResponse;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.pools.Poolable;

/**
 *  Interface implemented by the responses that are generated for procedure invocations
 */

public interface WorkflowResponse extends Poolable {
    /**
    * Get an estimate of the amount of time it took for the database
    * to process the stream workflow from the time it was received at the initiating node to the time
    * the ending node got the response and queued it for transmission to the client.
    * This time is an calculated from all the related transactions executed in the workflow
    * @return Time in milliseconds the workflow spent in the cluster
    */
   public int getClusterRoundtrip();
   
   /**
    * Retrieve the status code returned by the server
    * @return Status code
    */
   public Status getStatus();

   
   public void addClientResponse(final ClientResponse cresponse);

}
