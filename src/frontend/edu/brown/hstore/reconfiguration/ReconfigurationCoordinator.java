package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.commons.lang.NotImplementedException;
import org.apache.log4j.Logger;
import org.voltdb.VoltTable;

import weka.classifiers.meta.END;

import com.google.protobuf.RpcCallback;

import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.Hstoreservice.HStoreService;
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
    private String desiredPartitionPlan;
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
     * @return
     */
    public ReconfigurationPlan initReconfiguration(Integer leaderId, ReconfigurationProtocols reconfigurationProtocol, String partitionPlan, int partitionId) {
        if (reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
            if (partitionId != -1) {
                this.partitionStates.put(partitionId, ReconfigurationState.DATA_TRANSFER);
                this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
            } else {
                String msg = "No PARTITION ID set on init for stop and copy";
                LOG.error(msg);
                throw new RuntimeException(msg);
            }
        } else {
            throw new NotImplementedException();
        }
        if ((this.reconfigurationInProgress.get() == false) && (this.reconfigurationInProgress.compareAndSet(false, true))) {
            LOG.info("Initializing reconfiguration. New reconfig plan.");
            if (this.hstore_site.getSiteId() == leaderId) {
                if (debug.val) {
                    LOG.debug("Setting site as reconfig leader");
                }
            }
            this.reconfigurationLeader = leaderId;
            this.reconfigurationProtocol = reconfigurationProtocol;
            this.desiredPartitionPlan = partitionPlan;
            PlannedHasher hasher = (PlannedHasher) this.hstore_site.getHasher();
            ReconfigurationPlan reconfig_plan;
            try {
                // Find reconfig plan
                reconfig_plan = hasher.changePartitionPhase(partitionPlan);
                if (reconfigurationProtocol == ReconfigurationProtocols.STOPCOPY) {
                    // Nothing to do for S&C. PE's directly notified by
                    // sysProcedure
                } else {
                    if (reconfig_plan != null) {
                        for (PartitionExecutor executor : this.local_executors) {
                            executor.initReconfiguration(reconfig_plan, reconfigurationProtocol, ReconfigurationState.BEGIN);
                        }
                    }
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
                this.reconfigurationInProgress.set(false);
                this.currentReconfigurationPlan = null;
                this.reconfigurationLeader = -1;
                this.reconfigurationProtocol = null;
                resetPartitionsState();
            }
        } else {
            throw new NotImplementedException();
        }

    }

    private boolean allPartitionsFinished() {
        for(ReconfigurationState state : partitionStates.values()){
            if (state != ReconfigurationState.END)
                return false;
        }
        return true;            
    }
    
    private void resetPartitionsState(){
        this.partitionStates.putAll(this.initialPartitionStates);
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
     * Send
     * 
     * @param partitionId
     * @param new_partition
     * @param vt
     */
    public void pushTuples(int partitionId, int new_partition, String table_name, VoltTable vt) {
        // TODO Auto-generated method stub

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
                ReconfigurationRequest reconfigurationRequest = ReconfigurationRequest.newBuilder().setSenderSite(this.localSiteId).setT0S(System.currentTimeMillis()).build();

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

    private final RpcCallback<ReconfigurationResponse> reconfigurationRequestCallback = new RpcCallback<ReconfigurationResponse>() {
        @Override
        public void run(ReconfigurationResponse msg) {
            int senderId = msg.getSenderSite();
            ReconfigurationCoordinator.this.destinationsReady.add(senderId);
            if (ReconfigurationCoordinator.this.reconfigurationInProgress.get() && ReconfigurationCoordinator.this.reconfigurationState == ReconfigurationState.PREPARE
                    && ReconfigurationCoordinator.this.destinationsReady.size() == ReconfigurationCoordinator.this.destinationSize) {
                ReconfigurationCoordinator.this.reconfigurationState = ReconfigurationState.DATA_TRANSFER;
                // bulk data transfer for stop and copy after each destination
                // is ready
                ReconfigurationCoordinator.this.bulkDataTransfer();
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

    public String getDesiredPartitionPlan() {
        return this.desiredPartitionPlan;
    }

}
