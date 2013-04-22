package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Set;

import org.apache.log4j.Logger;
import org.hsqldb.lib.HashSet;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.ReconfigurationRequest;
import edu.brown.hstore.Hstoreservice.ReconfigurationResponse;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

/**
 * 
 * @author vaibhav : Reconfiguration Coordinator at each site, responsible for 
 * maintaining reconfiguration state and sending communication messages 
 *
 */
public class ReconfigurationCoordinator {
  private static final Logger LOG = Logger.getLogger(ReconfigurationCoordinator.class);
  private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
  private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
  static {
      LoggerUtil.setupLogging();
      LoggerUtil.attachObserver(LOG, debug, trace);
  }
  
  public enum ReconfigurationState {
    BEGIN,
    PREPARE,
    DATA_TRANSFER,
    END
  }
  
  private ReconfigurationState reconfigurationState;
  // Hostname of the reconfiguration leader site
  private Integer reconfigurationLeader; 
  private boolean reconfigurationInProgress;
  private ReconfigurationProtocols reconfigurationProtocol;
  private String desiredPartitionPlan;
  private int localSiteId;
  private HStoreService channels[];
  private Set<Integer> destinationsReady;
  private int destinationSize;
  
  public ReconfigurationCoordinator(int localSiteId, HStoreService[] channels){
    this.reconfigurationInProgress = false;
    this.localSiteId = localSiteId;
    this.channels = channels;
    this.destinationsReady = new java.util.HashSet<Integer>();
  }
  
  /**
   * Returns true and initiates a reconfiguration if a current one is not going
   * If a reconfiguration is going returns false
   * @param leaderHostname
   * @return
   */
  public boolean initReconfiguration(Integer leaderId, ReconfigurationProtocols 
      reconfigurationProtocol,
      String partitionPlan){
    if(!this.reconfigurationInProgress){
      this.reconfigurationState = ReconfigurationState.BEGIN;
      this.reconfigurationLeader = leaderId;
      this.reconfigurationProtocol = reconfigurationProtocol;
      this.reconfigurationInProgress = true;
      this.desiredPartitionPlan = partitionPlan;
      return true;
    } else {
      return false;
    } 
  }
  
  /**
   * For live pull protocol move the state to Data Transfer Mode
   * For Stop and Copy, move reconfiguration into Prepare Mode
   */
  public void prepareReconfiguration(){
    if(this.reconfigurationInProgress){
      if(this.reconfigurationProtocol == ReconfigurationProtocols.LIVEPULL){
        // Move the reconfiguration state to data transfer and data will be pulled based on
        // demand form the destination
        this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
      } else if(this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY){
        //First set the state to send control messages 
        this.reconfigurationState = ReconfigurationState.PREPARE;
        sendPrepare(findDestinationSites());
      }
    }
  }
  
  /**
   * Parse the partition plan and figure out the destination sites
   * and populates the destination size
   * @return
   */
  public ArrayList<Integer> findDestinationSites(){
    ArrayList<Integer> destinationSites = new ArrayList<Integer>();
    
    //TODO : Populate the destinationSize as well
    
    return destinationSites;
  }
  
  /**
   * Send prepare messages to all destination sites for Stop and Copy
   * @param destinationHostNames
   */
  public void sendPrepare(ArrayList<Integer> destinationSites){
    if(this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY){
      for(Integer destinationId : destinationSites){
        // Send a control message to start the reconfiguration
        
        ProtoRpcController controller = new ProtoRpcController();
        ReconfigurationRequest reconfigurationRequest = ReconfigurationRequest.newBuilder().
            setSenderSite(this.localSiteId).setT0S(System.currentTimeMillis()).build();
        
        this.channels[destinationId].reconfiguration(controller, reconfigurationRequest, 
            this.reconfigurationRequestCallback);
      }
    }
  }
  
  /**
   * used for bulk transfer during stop and copy
   */
  public void bulkDataTransfer(){
    if(this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY){
      // bulk transfer the table data 
      // by calling on transfer for each partition of the site
    }
  }
  
  /**
   * Invoked after receiving messages from reconfiguration leader signaling end of reconfiguration
   */
  public void endReconfiguration(){
    this.reconfigurationInProgress = false;
  }
  
  private final RpcCallback<ReconfigurationResponse> reconfigurationRequestCallback = 
      new RpcCallback<ReconfigurationResponse>() {
    @Override
    public void run(ReconfigurationResponse msg) {
      int senderId = msg.getSenderSite();
      destinationsReady.add(senderId);
      if(reconfigurationInProgress && reconfigurationState == ReconfigurationState.PREPARE
          && destinationsReady.size() == destinationSize){
        reconfigurationState = ReconfigurationState.DATA_TRANSFER;
        // bulk data transfer for stop and copy after each destination is ready
        bulkDataTransfer();
      }
    }
  };
  
  public ReconfigurationState getState(){
    return this.reconfigurationState;
  }
  
  public Integer getReconfigurationLeader(){
    return this.reconfigurationLeader;
  }
  
  public ReconfigurationProtocols getReconfigurationProtocol(){
    return this.reconfigurationProtocol;
  }
  
  public String getDesiredPartitionPlan(){
    return this.desiredPartitionPlan;
  }
  
}
