package edu.brown.hstore.reconfiguration;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.messaging.FastDeserializer;
import org.voltdb.messaging.FastSerializer;

import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;

import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hashing.ReconfigurationPlan.ReconfigurationRange;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.DataTransferRequest;
import edu.brown.hstore.Hstoreservice.DataTransferResponse;
import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.LivePullRequest;
import edu.brown.hstore.Hstoreservice.LivePullResponse;
import edu.brown.hstore.Hstoreservice.ReconfigurationRequest;
import edu.brown.hstore.Hstoreservice.ReconfigurationResponse;
import edu.brown.hstore.PartitionExecutor;
import edu.brown.hstore.reconfiguration.ReconfigurationConstants.ReconfigurationProtocols;
import edu.brown.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.protorpc.ProtoRpcController;

/**
 * @author vaibhav : Reconfiguration Coordinator at each site, responsible for
 *         maintaining reconfiguration state and sending communication messages
 */
/**
 * @author aelmore
 */
public class ReconfigurationCoordinator implements Shutdownable {
    private static final Logger LOG = Logger.getLogger(ReconfigurationCoordinator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());

    // Cached list of local executors
    private List<PartitionExecutor> local_executors;
    static {
        LoggerUtil.setupLogging();
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public enum ReconfigurationState {
        NORMAL, BEGIN, PREPARE, DATA_TRANSFER, END
    }

    private HStoreSite hstore_site;

    private ReconfigurationState reconfigurationState;
    // Hostname of the reconfiguration leader site
    private Integer reconfigurationLeader;
    private AtomicBoolean reconfigurationInProgress;
    private ReconfigurationPlan currentReconfigurationPlan;
    private ReconfigurationProtocols reconfigurationProtocol;
    private String currentPartitionPlan;
    private int localSiteId;
    private HStoreService channels[];
    private Set<Integer> destinationsReady;
    private int destinationSize;
    // Map of partitions in a reconfiguration and their state. No entry is not
    // in reconfiguration;
    private Map<Integer, ReconfigurationState> partitionStates;
    private Map<Integer, ReconfigurationState> initialPartitionStates;

    public ReconfigurationCoordinator(HStoreSite hstore_site) {
        // TODO Auto-generated constructor stub
        this.reconfigurationLeader = -1;
        this.reconfigurationInProgress = new AtomicBoolean(false);
        this.currentReconfigurationPlan = null;
        this.reconfigurationState = ReconfigurationState.NORMAL;
        this.hstore_site = hstore_site;
        this.local_executors = new ArrayList<>();
        this.channels = hstore_site.getCoordinator().getChannels();
        this.partitionStates = new ConcurrentHashMap<Integer, ReconfigurationCoordinator.ReconfigurationState>();
        for (int p_id : hstore_site.getLocalPartitionIds().values()) {
            this.local_executors.add(hstore_site.getPartitionExecutor(p_id));
            this.partitionStates.put(p_id, ReconfigurationState.NORMAL);
        }
        this.initialPartitionStates = Collections.unmodifiableMap(partitionStates);
    }

    /**
     * Initialize a reconfiguration. May be called by multiple PEs, so first
     * request initializes and caches plan. Additional requests will be given
     * cached plan.
     * 
     * @param leaderId
     * @param reconfigurationProtocol
     * @param partitionPlan
     * @param partitionId
     * @return the reconfiguration plan or null if plan already set
     */
    public ReconfigurationPlan initReconfiguration(Integer leaderId, ReconfigurationProtocols reconfigurationProtocol, String partitionPlan, int partitionId) {
        
        //TODO ae start timing
        if (this.reconfigurationInProgress.get() == false && partitionPlan == this.currentPartitionPlan) {
            LOG.info("Ignoring initReconfiguration request. Requested plan is already set");
            return null;
        }
        if (reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
            if (partitionId != -1) {
                this.partitionStates.put(partitionId, ReconfigurationState.DATA_TRANSFER);
                this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
            } else {
                String msg = "No PARTITION ID set on init for stop and copy";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
        } else if (reconfigurationProtocol == ReconfigurationProtocols.LIVEPULL) {
            
        } else {
            throw new NotImplementedException();
        }

        // We may have reconfiguration initialized by PEs so need to ensure
        // atomic
        if (this.reconfigurationInProgress.compareAndSet(false, true)) {
            LOG.info("Initializing reconfiguration. New reconfig plan.");
            if (this.hstore_site.getSiteId() == leaderId) {
                LOG.error("TODO setting as leader");
                // TODO : Check if more leader logic is needed
                if (debug.val) {
                    LOG.debug("Setting site as reconfig leader");
                }
            }
            this.reconfigurationLeader = leaderId;
            this.reconfigurationProtocol = reconfigurationProtocol;
            this.currentPartitionPlan = partitionPlan;
            PlannedHasher hasher = (PlannedHasher) this.hstore_site.getHasher();
            ReconfigurationPlan reconfig_plan;
            try {
                // Find reconfig plan
                reconfig_plan = hasher.changePartitionPhase(partitionPlan);
                if (reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
                    // Nothing to do for S&C. PE's directly notified by
                    // sysProcedure
                } else if(reconfigurationProtocol == ReconfigurationProtocols.LIVEPULL) {
                    if (reconfig_plan != null) {
                        for (PartitionExecutor executor : this.local_executors) {
                            executor.initReconfiguration(reconfig_plan, reconfigurationProtocol, ReconfigurationState.PREPARE);
                            this.partitionStates.put(partitionId, ReconfigurationState.PREPARE);
                        }
                        //Notify leader that this node has been initialized / prepared
                        notifyReconfigLeader(ReconfigurationState.PREPARE);
                    } else {
                        LOG.info("No reconfig plan, nothing to do");
                    }
                } else { 
                    throw new NotImplementedException();
                }
            } catch (Exception e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }
            this.currentReconfigurationPlan = reconfig_plan;
            return reconfig_plan;
        } else {
            // If the reconfig plan is null, but we are in progress we should
            // re-attempt to get it;
            int tries = 0;
            int max_tries = 20;
            long sleep_time = 50;
            while (this.currentReconfigurationPlan == null && tries < max_tries) {
                try {
                    Thread.sleep(sleep_time);
                    tries++;
                } catch (InterruptedException e) {
                    LOG.error("Error sleeping", e);
                }
            }
            LOG.debug(String.format("Init reconfiguration returning existing plan %s", this.currentReconfigurationPlan));

            return this.currentReconfigurationPlan;
        }
    }

    /**
     * Notify leader of a state change
     * @param begin
     */
    private void notifyReconfigLeader(ReconfigurationState state) {
        assert(this.reconfigurationLeader != -1 ) : "No reconfiguration leader set";
        LOG.info("Notifying reconfiguration leader of state:"+state.toString());
        if (this.hstore_site.getSiteId() == this.reconfigurationLeader){
            //This node is the leader
        }
        else{
            //TODO send state notification to leader
        }
        
    }

    /**
     * Function called by a PE when its active part of the reconfiguration is
     * complete
     * 
     * @param partitionId
     */
    public void finishReconfiguration(int partitionId) {
        if (this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
            this.partitionStates.remove(partitionId);

            if (allPartitionsFinished()) {
                LOG.info("Last PE finished reconfiguration");
                resetReconfigurationInProgress();
            }
        } else {
            throw new NotImplementedException();
        }

    }

    private boolean allPartitionsFinished() {
        for (ReconfigurationState state : partitionStates.values()) {
            if (state != ReconfigurationState.END)
                return false;
        }
        return true;
    }

    private void resetReconfigurationInProgress() {
        this.partitionStates.putAll(this.initialPartitionStates);
        this.currentReconfigurationPlan = null;
        this.reconfigurationLeader = -1;
        this.reconfigurationProtocol = null;
        this.reconfigurationInProgress.set(false);
    }

    /**
     * For live pull protocol move the state to Data Transfer Mode For Stop and
     * Copy, move reconfiguration into Prepare Mode
     */
    public void prepareReconfiguration() {
        if (this.reconfigurationInProgress.get()) {
            if (this.reconfigurationProtocol == ReconfigurationProtocols.LIVEPULL) {
                // Move the reconfiguration state to data transfer and data will
                // be
                // pulled based on
                // demand form the destination
                this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
            } else if (this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
                // First set the state to send control messages
                this.reconfigurationState = ReconfigurationState.PREPARE;
                this.sendPrepare(this.findDestinationSites());
            }
        }
    }

  
    /**
     * @param oldPartitionId
     * @param newPartitionId
     * @param table_name
     * @param vt
     * @throws Exception
     */
    public void pushTuples(int oldPartitionId, int newPartitionId, String table_name, VoltTable vt) throws Exception {
        LOG.info(String.format("pushTuples  keys for %s  partIds %->%s",table_name,oldPartitionId,newPartitionId));
        // TODO Auto-generated method stub
        int destinationId = this.hstore_site.getCatalogContext().getSiteIdForPartitionId(newPartitionId);

        if (destinationId == localSiteId) {
            // Just push the message through local receive Tuples to the PE'S
            receiveTuples(destinationId, System.currentTimeMillis(), oldPartitionId, newPartitionId, table_name, vt);
            return;
        }

        ProtoRpcController controller = new ProtoRpcController();
        ByteString tableBytes = null;
        try {
            ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(vt));
            tableBytes = ByteString.copyFrom(b.array());
        } catch (Exception ex) {
            throw new RuntimeException("Unexpected error when serializing Volt Table", ex);
        }

        DataTransferRequest dataTransferRequest = DataTransferRequest.newBuilder().setSenderSite(this.localSiteId).
            setOldPartition(oldPartitionId).setNewPartition(newPartitionId)
                .setVoltTableName(table_name).setT0S(System.currentTimeMillis()).setVoltTableData(tableBytes).build();

        this.channels[destinationId].dataTransfer(controller, dataTransferRequest, dataTransferRequestCallback);
    }

    /**
     * Receive the tuples
     * 
     * @param partitionId
     * @param newPartitionId
     * @param table_name
     * @param vt
     * @throws Exception
     */
    public DataTransferResponse receiveTuples(int sourceId, long sentTimeStamp, int partitionId, int newPartitionId, 
        String table_name, VoltTable vt) throws Exception {
        LOG.info(String.format("receiveTuples  keys for %s  partIds %->%s",table_name,sourceId,newPartitionId));
        
        if (vt == null) {
            LOG.error("Volt Table received is null");
        }

        for (PartitionExecutor executor : this.local_executors) {
            // TODO : check if we can more efficient here
            if (executor.getPartitionId() == newPartitionId) {
                executor.receiveTuples(partitionId, newPartitionId, table_name, vt);
            }
        }

        DataTransferResponse response = null;

        response = DataTransferResponse.newBuilder().setNewPartition(newPartitionId).setOldPartition(partitionId).
            setT0S(sentTimeStamp).setSenderSite(sourceId).build();

        return response;
    }

    /**
     * Call to send tuples in response to a live pull request
     * coming into the system
     * @param senderId
     * @param requestTimestamp
     * @param txnId
     * @param oldPartitionId
     * @param newPartitionId
     * @param table_name
     * @param min_inclusive
     * @param max_exclusive
     * @param voltType
     * @return
     */
    public LivePullResponse sendTuples(int senderId, Long requestTimestamp, Long txnId, 
        int oldPartitionId, int newPartitionId, String table_name, Long min_inclusive, 
        Long max_exclusive){
      LOG.info(String.format("sendTuples  keys %->%s for %s  partIds %->%s",min_inclusive,max_exclusive, table_name,oldPartitionId,newPartitionId));
        
      VoltTable vt = null;
      for (PartitionExecutor executor : this.local_executors) {
        // TODO : check if we can more efficient here
        if (executor.getPartitionId() == newPartitionId) {
            vt = executor.sendTuples(txnId, oldPartitionId, newPartitionId, table_name, min_inclusive, 
                max_exclusive);
        }
      }
      
      ByteString tableBytes = null;
      try {
          ByteBuffer b = ByteBuffer.wrap(FastSerializer.serialize(vt));
          tableBytes = ByteString.copyFrom(b.array());
      } catch (Exception ex) {
          throw new RuntimeException("Unexpected error when serializing Volt Table", ex);
      }
      
      LivePullResponse livePullResponse = LivePullResponse.newBuilder().setSenderSite(this.localSiteId).
          setOldPartition(oldPartitionId).setNewPartition(newPartitionId)
              .setVoltTableName(table_name).setT0S(System.currentTimeMillis()).setVoltTableData(tableBytes)
              .setMinInclusive(min_inclusive).setMaxExclusive(max_exclusive)
              .setTransactionID(txnId).build();
      
      return livePullResponse;
    }
    
   /**
    * Live Pull the tuples for a reconfiguration range
    * by generating a live pull request
    * @param txnId
    * @param oldPartitionId
    * @param newPartitionId
    * @param table_name
    * @param min_inclusive
    * @param max_exclusive
    * @param voltType
    */
    public void pullTuples(Long txnId, int oldPartitionId, int newPartitionId, String table_name, 
        Long min_inclusive, Long max_exclusive,
        VoltType voltType){
      LOG.info(String.format("pullTuples  keys %->%s for %s  partIds %->%s",min_inclusive,max_exclusive, table_name,oldPartitionId,newPartitionId));
      //TODO : Check if volt type makes can be used here for generic values or remove it
      int destinationId = this.hstore_site.getCatalogContext().getSiteIdForPartitionId(newPartitionId);

      if (destinationId == localSiteId) {
          // Just push the message through local receive Tuples to the PE'S
          sendTuples(destinationId, System.currentTimeMillis(), txnId, 
              oldPartitionId, newPartitionId, table_name, min_inclusive, 
              max_exclusive);
          return;
      }
      
      ProtoRpcController controller = new ProtoRpcController();

      LivePullRequest livePullRequest = LivePullRequest.newBuilder().setSenderSite(this.localSiteId).
          setTransactionID(txnId).setOldPartition(oldPartitionId).setNewPartition(newPartitionId).
          setVoltTableName(table_name).setMinInclusive(min_inclusive).
          setMaxExclusive(max_exclusive).
          setT0S(System.currentTimeMillis()).build();

      this.channels[destinationId].livePull(controller, livePullRequest, livePullRequestCallback);
    }
    
    
    /**
     * Parse the partition plan and figure out the destination sites and
     * populates the destination size
     * 
     * @return
     */
    public ArrayList<Integer> findDestinationSites() {
        ArrayList<Integer> destinationSites = new ArrayList<Integer>();

        // TODO : Populate the destinationSize as well

        return destinationSites;
    }

    /**
     * Send prepare messages to all destination sites for Stop and Copy
     * 
     * @param destinationHostNames
     */
    public void sendPrepare(ArrayList<Integer> destinationSites) {
        if (this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
            for (Integer destinationId : destinationSites) {
                // Send a control message to start the reconfiguration

                ProtoRpcController controller = new ProtoRpcController();
                ReconfigurationRequest reconfigurationRequest = ReconfigurationRequest.newBuilder().setSenderSite(this.localSiteId).
                    setT0S(System.currentTimeMillis()).build();

                this.channels[destinationId].reconfiguration(controller, reconfigurationRequest, this.reconfigurationRequestCallback);
            }
        }
    }

    @Override
    public void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub

    }

    @Override
    public void shutdown() {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isShuttingDown() {
        // TODO Auto-generated method stub
        return false;
    }

    /**
     * used for bulk transfer during stop and copy
     */
    public void bulkDataTransfer() {
        if (this.reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
            // bulk transfer the table data
            // by calling on transfer for each partition of the site
        }
    }

    /**
     * Invoked after receiving messages from reconfiguration leader signaling
     * end of reconfiguration
     */
    public void endReconfiguration() {
        this.reconfigurationInProgress.set(false);
    }

    private final RpcCallback<ReconfigurationResponse> reconfigurationRequestCallback = new 
        RpcCallback<ReconfigurationResponse>() {
        @Override
        public void run(ReconfigurationResponse msg) {
            int senderId = msg.getSenderSite();
            ReconfigurationCoordinator.this.destinationsReady.add(senderId);
            if (ReconfigurationCoordinator.this.reconfigurationInProgress.get() && ReconfigurationCoordinator.
                this.reconfigurationState == ReconfigurationState.PREPARE
                    && ReconfigurationCoordinator.this.destinationsReady.size() == ReconfigurationCoordinator.this.destinationSize) {
                ReconfigurationCoordinator.this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
                // bulk data transfer for stop and copy after each destination
                // is ready
                ReconfigurationCoordinator.this.bulkDataTransfer();
            }
        }
    };

    private final RpcCallback<DataTransferResponse> dataTransferRequestCallback = new 
        RpcCallback<DataTransferResponse>() {
        @Override
        public void run(DataTransferResponse msg) {
            // TODO : Do the book keeping of received messages
            int senderId = msg.getSenderSite();
            int oldPartition = msg.getOldPartition();
            int newPartition = msg.getNewPartition();
            long timeStamp = msg.getT0S();

        }
    };
    
    private final RpcCallback<LivePullResponse> livePullRequestCallback = 
        new RpcCallback<LivePullResponse>() {
      @Override
      public void run(LivePullResponse msg) {
          // TODO : Do the book keeping of received messages
          int senderId = msg.getSenderSite();
          long timeStamp = msg.getT0S();
          long txnId = msg.getT0S();
          LOG.debug("Received senderId "+senderId+" timestamp "+timeStamp+" Transaction Id "+txnId);
          VoltTable vt = null;
          try {
            vt = FastDeserializer.deserialize(msg.getVoltTableData().toByteArray(), VoltTable.class);
          } catch (IOException e) {
            LOG.error("Error in deserializing volt table");
          }
          //TODO : change the log status later. Info for testing.
          LOG.info("Volt table Received in callback is "+vt.toString());
          
          for (PartitionExecutor executor : local_executors) {
            // TODO : check if we can more efficient here
            if (executor.getPartitionId() == msg.getOldPartition()) {
                try {
                  executor.receiveTuples(msg.getTransactionID(), 
                      msg.getOldPartition(), msg.getNewPartition(),
                      msg.getVoltTableName(), msg.getMinInclusive(), msg.getMaxExclusive(),
                      vt);
                } catch (Exception e) {
                  LOG.error("Error in partition executors receiving tuples for their pull " +
                  		"request", e);
                }
            }
        }
      }
  };

    public ReconfigurationState getState() {
        return this.reconfigurationState;
    }

    public Integer getReconfigurationLeader() {
        return this.reconfigurationLeader;
    }

    public ReconfigurationProtocols getReconfigurationProtocol() {
        return this.reconfigurationProtocol;
    }

    public String getCurrentPartitionPlan() {
        return this.currentPartitionPlan;
    }

}
