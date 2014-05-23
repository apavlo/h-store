package edu.brown.hstore;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.hstore.Hstoreservice.HStoreService;
import edu.brown.hstore.Hstoreservice.HeartbeatRequest;
import edu.brown.hstore.Hstoreservice.HeartbeatResponse;
import edu.brown.hstore.Hstoreservice.InitializeRequest;
import edu.brown.hstore.Hstoreservice.InitializeResponse;
import edu.brown.hstore.Hstoreservice.SendDataRequest;
import edu.brown.hstore.Hstoreservice.SendDataResponse;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareRequest;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareResponse;
import edu.brown.hstore.Hstoreservice.ShutdownRequest;
import edu.brown.hstore.Hstoreservice.ShutdownResponse;
import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TimeSyncRequest;
import edu.brown.hstore.Hstoreservice.TimeSyncResponse;
import edu.brown.hstore.Hstoreservice.TransactionDebugRequest;
import edu.brown.hstore.Hstoreservice.TransactionDebugResponse;
import edu.brown.hstore.Hstoreservice.TransactionFinishRequest;
import edu.brown.hstore.Hstoreservice.TransactionFinishResponse;
import edu.brown.hstore.Hstoreservice.TransactionInitRequest;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.Hstoreservice.TransactionMapRequest;
import edu.brown.hstore.Hstoreservice.TransactionMapResponse;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchAcknowledgement;
import edu.brown.hstore.Hstoreservice.TransactionPrefetchResult;
import edu.brown.hstore.Hstoreservice.TransactionPrepareRequest;
import edu.brown.hstore.Hstoreservice.TransactionPrepareResponse;
import edu.brown.hstore.Hstoreservice.TransactionRedirectRequest;
import edu.brown.hstore.Hstoreservice.TransactionRedirectResponse;
import edu.brown.hstore.Hstoreservice.TransactionReduceRequest;
import edu.brown.hstore.Hstoreservice.TransactionReduceResponse;
import edu.brown.hstore.Hstoreservice.TransactionWorkRequest;
import edu.brown.hstore.Hstoreservice.TransactionWorkResponse;
import edu.brown.hstore.Hstoreservice.UnevictDataRequest;
import edu.brown.hstore.Hstoreservice.UnevictDataResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.StringUtil;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.RemoteTransaction;

public class MockHStoreCoordinator extends HStoreCoordinator {
    private static final Logger LOG = Logger.getLogger(MockHStoreCoordinator.class);
    private final static LoggerBoolean debug = new LoggerBoolean();
    private final static LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    final TransactionQueueManager txnQueueManager;
    final HStoreConf hstore_conf;
    final HStoreSite hstore_site;
    
    /**
     * Constructor
     * @param catalog_site
     */
    public MockHStoreCoordinator(final MockHStoreSite hstore_site) {
        super(hstore_site);
        this.hstore_site = hstore_site;
        this.hstore_conf = this.hstore_site.getHStoreConf();
        this.txnQueueManager = this.hstore_site.getTransactionQueueManager();
        
        Thread t = new Thread(this.txnQueueManager);
        t.setDaemon(true);
        t.start();
        
        // For debugging!
        this.getReadyObservable().addObserver(new EventObserver<HStoreCoordinator>() {
            @Override
            public void update(EventObservable<HStoreCoordinator> o, HStoreCoordinator arg) {
                if (HStoreCoordinator.getRemoteCoordinators(hstore_site.getSite()).isEmpty() == false) {
                    LOG.info("Established connections to remote HStoreCoordinators:\n" +
                             StringUtil.join("  ", "\n", HStoreCoordinator.getRemoteCoordinators(hstore_site.getSite())));
                }
            }
        });
        
//        Runtime.getRuntime().addShutdownHook(new Thread() {
//            @Override
//            public void run() {
//                if (isShuttingDown() == false) {
//                    shutdownCluster();
//                    System.out.println("Shutdown hook ran!");
//                }
//            }
//        });
    }
    
    @Override
    protected HStoreService initHStoreService() {
        return new MockServiceHandler();
    }
    @Override
    protected void initCluster() {
        // Nothing to do...
    }
    
    private class MockServiceHandler extends HStoreService {

        @Override
        public void transactionInit(RpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            PartitionSet partitions = new PartitionSet(request.getPartitionsList());
            RemoteTransaction ts = hstore_site.getTransactionInitializer()
                                             .createRemoteTransaction(request.getTransactionId(),
                                                                      partitions,
                                                                      null,
                                                                      request.getBasePartition(),
                                                                      request.getProcedureId());
            // FIXME hstore_site.transactionInit(ts, done);
        }

        @Override
        public void transactionWork(RpcController controller, TransactionWorkRequest request, RpcCallback<TransactionWorkResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionPrepare(RpcController controller, TransactionPrepareRequest request, RpcCallback<TransactionPrepareResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionFinish(RpcController controller, TransactionFinishRequest request, RpcCallback<TransactionFinishResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            // TODO Auto-generated method stub
        }

        @Override
        public void transactionRedirect(RpcController controller, TransactionRedirectRequest request, RpcCallback<TransactionRedirectResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            // Ignore
        }
        
        @Override
        public void initialize(RpcController controller, InitializeRequest request, RpcCallback<InitializeResponse> done) {
            // TODO Auto-generated method stub
        }

        @Override
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            ShutdownResponse response = ShutdownResponse.newBuilder()
                                                     .setSenderSite(hstore_site.getSiteId())
                                                     .build();
            System.exit(1);
            done.run(response);
            
        }
        
        @Override
        public void heartbeat(RpcController controller, HeartbeatRequest request, RpcCallback<HeartbeatResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void timeSync(RpcController controller, TimeSyncRequest request, RpcCallback<TimeSyncResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionMap(RpcController controller, TransactionMapRequest request, RpcCallback<TransactionMapResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionReduce(RpcController controller, TransactionReduceRequest request, RpcCallback<TransactionReduceResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void sendData(RpcController controller, SendDataRequest request, RpcCallback<SendDataResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionPrefetch(RpcController controller, TransactionPrefetchResult request,
                RpcCallback<TransactionPrefetchAcknowledgement> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void shutdownPrepare(RpcController controller, ShutdownPrepareRequest request, RpcCallback<ShutdownPrepareResponse> done) {
            // TODO Auto-generated method stub
            
        }

        @Override
        public void transactionDebug(RpcController controller, TransactionDebugRequest request, RpcCallback<TransactionDebugResponse> done) {
            // TODO Auto-generated method stub
            
        }

		@Override
		public void unevictData(RpcController controller,
				UnevictDataRequest request,
				RpcCallback<UnevictDataResponse> done) {
			LOG.info(String.format("Received %s from HStoreSite %s at HStoreSite %s",
                    request.getClass().getSimpleName(),
                    HStoreThreadManager.formatSiteName(request.getSenderSite()),
                    HStoreThreadManager.formatSiteName(hstore_site.getSiteId())));

            if (debug.val)
                LOG.debug(String.format("Received %s from HStoreSite %s",
                          request.getClass().getSimpleName(),
                          HStoreThreadManager.formatSiteName(request.getSenderSite())));
            Long oldTxnId = request.getTransactionId();
            UnevictDataResponse.Builder builder = UnevictDataResponse.newBuilder()
                                                    .setSenderSite(hstore_site.getSiteId())
                                                    .setTransactionId(oldTxnId)
                                                    .setPartitionId(1) // some id
                                                    .setStatus(Status.OK);
            done.run(builder.build());   

			
		}
    }

}
