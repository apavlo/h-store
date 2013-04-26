package edu.brown.hstore.reconfiguration;

import java.util.ArrayList;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;
import org.hsqldb.lib.HashSet;
import org.voltdb.VoltTable;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.ReconfigurationRequest;
import edu.brown.hstore.Hstoreservice.ReconfigurationResponse;
import edu.brown.hashing.PlannedHasher;
import edu.brown.hashing.ReconfigurationPlan;
import edu.brown.hstore.HStoreSite;
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
        BEGIN, PREPARE, DATA_TRANSFER, END
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

    public ReconfigurationCoordinator(HStoreSite hstore_site) {
        // TODO Auto-generated constructor stub
        this.reconfigurationInProgress.set(false);
        currentReconfigurationPlan = null;
        reconfigurationState = ReconfigurationState.END;
        this.hstore_site = hstore_site;
        local_executors = new ArrayList<>();
        this.channels = hstore_site.getCoordinator().getChannels();
        for (int p_id : hstore_site.getLocalPartitionIds().values()) {
            local_executors.add(hstore_site.getPartitionExecutor(p_id));
        }
    }

    /**
     * Returns true and initiates a reconfiguration if a current one is not
     * going If a reconfiguration is going returns false
     * 
     * @param leaderHostname
     * @return
     */
    public ReconfigurationPlan initReconfiguration(Integer leaderId, ReconfigurationProtocols reconfiguration_protocol, String partitionPlan) {
        if ((reconfigurationInProgress.get() == false) && (this.reconfigurationInProgress.compareAndSet(false, true))) {
            LOG.info("Initializing reconfiguration. New reconfig plan.");
            if (this.hstore_site.getSiteId() == leaderId) {
                if (debug.val)
                    LOG.debug("Setting site as reconfig leader");
            }
            this.reconfigurationState = ReconfigurationState.BEGIN;
            this.reconfigurationLeader = leaderId;
            this.reconfigurationProtocol = reconfiguration_protocol;
            this.desiredPartitionPlan = partitionPlan;
            PlannedHasher hasher = (PlannedHasher) hstore_site.getHasher();
            ReconfigurationPlan reconfig_plan;
            try {
                reconfig_plan = hasher.changePartitionPhase(partitionPlan);
                if (reconfig_plan != null) {
                    for (PartitionExecutor executor : local_executors) {
                        executor.initReconfiguration(reconfig_plan, reconfiguration_protocol, ReconfigurationState.BEGIN);
                    }
                }
            } catch (Exception e) {
                LOG.error(e);
                throw new RuntimeException(e);
            }

            return reconfig_plan;
        } else {
            LOG.info("Init reconfiguration returning existing plan");
            
            return this.currentReconfigurationPlan;
        }
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
                sendPrepare(findDestinationSites());
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
            destinationsReady.add(senderId);
            if (reconfigurationInProgress.get() && reconfigurationState == ReconfigurationState.PREPARE && destinationsReady.size() == destinationSize) {
                reconfigurationState = ReconfigurationState.DATA_TRANSFER;
                // bulk data transfer for stop and copy after each destination
                // is ready
                bulkDataTransfer();
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
