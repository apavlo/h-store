package edu.mit.hstore;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.ExecutionSite;
import org.voltdb.MockExecutionSite;
import org.voltdb.StoredProcedureInvocation;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.HStoreService;
import edu.brown.hstore.Hstore.ShutdownRequest;
import edu.brown.hstore.Hstore.ShutdownResponse;
import edu.brown.hstore.Hstore.Status;
import edu.brown.hstore.Hstore.TransactionFinishRequest;
import edu.brown.hstore.Hstore.TransactionFinishResponse;
import edu.brown.hstore.Hstore.TransactionInitRequest;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.hstore.Hstore.TransactionPrepareRequest;
import edu.brown.hstore.Hstore.TransactionPrepareResponse;
import edu.brown.hstore.Hstore.TransactionRedirectRequest;
import edu.brown.hstore.Hstore.TransactionRedirectResponse;
import edu.brown.hstore.Hstore.TransactionWorkRequest;
import edu.brown.hstore.Hstore.TransactionWorkResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.StringUtil;
import edu.mit.hstore.HStoreConf;
import edu.mit.hstore.HStoreCoordinator;
import edu.mit.hstore.HStoreSite;
import edu.mit.hstore.callbacks.TransactionInitWrapperCallback;
import edu.mit.hstore.dtxn.LocalTransaction;
import edu.mit.hstore.util.TransactionQueueManager;

public class MockCoordinator extends HStoreCoordinator {
    private static final Logger LOG = Logger.getLogger(MockCoordinator.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    
    private static class MockHStoreSite extends HStoreSite {
        public MockHStoreSite(Site catalog_site, PartitionEstimator p_estimator) {
            super(catalog_site,
                  makeExecutors(catalog_site, p_estimator),
                  p_estimator);
        }
        @Override
        public void transactionRestart(LocalTransaction orig_ts, Status status) {
            int restart_limit = 10;
            if (orig_ts.getRestartCounter() > restart_limit) {
                if (orig_ts.sysproc) {
                    throw new RuntimeException(String.format("%s has been restarted %d times! Rejecting...",
                                               orig_ts, orig_ts.getRestartCounter()));
                } else {
                    this.transactionReject(orig_ts, Hstore.Status.ABORT_REJECT);
                    return;
                }
            }
        }
    }
    
    static Map<Integer, ExecutionSite> makeExecutors(Site catalog_site, PartitionEstimator p_estimator) {
        Map<Integer, ExecutionSite> m = new HashMap<Integer, ExecutionSite>(); 
        for (Integer p : CatalogUtil.getLocalPartitionIds(catalog_site)) {
            m.put(p, new MockExecutionSite(p, catalog_site.getCatalog(), p_estimator));
        }
        return (m);
    }
    static PartitionEstimator makePartitionEstimator(Site catalog_site) {
        return new PartitionEstimator(CatalogUtil.getDatabase(catalog_site));
    }
    static LocalTransaction makeLocalTransaction(HStoreSite hstore_site) {
        long txnId = hstore_site.getTransactionIdManager().getNextUniqueTransactionId();
        long clientHandle = -1;
        int base_partition = CollectionUtil.random(hstore_site.getLocalPartitionIds());
        Collection<Integer> predict_touchedPartitions = hstore_site.getAllPartitionIds();
        boolean predict_readOnly = false;
        boolean predict_canAbort = true;
        TransactionEstimator.State estimator_state = null;
        Procedure catalog_proc = CollectionUtil.random(hstore_site.getDatabase().getProcedures());
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(clientHandle, catalog_proc.getName());
        RpcCallback<byte[]> client_callback = null;
        
        LocalTransaction ts = new LocalTransaction(hstore_site);
        ts.init(txnId, clientHandle, base_partition,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                estimator_state, catalog_proc, invocation, client_callback);
        return (ts);
    }
    
    final TransactionQueueManager txnManager;
    final HStoreConf hstore_conf;
    
    /**
     * Constructor
     * @param catalog_site
     */
    public MockCoordinator(final Site catalog_site) {
        super(new MockHStoreSite(catalog_site, makePartitionEstimator(catalog_site)));
        this.hstore_conf = this.getHStoreConf();
        this.txnManager = new TransactionQueueManager(this.getHStoreSite());
        
        // For debugging!
        this.getReadyObservable().addObserver(new EventObserver<HStoreCoordinator>() {
            @Override
            public void update(EventObservable<HStoreCoordinator> o, HStoreCoordinator arg) {
                LOG.info("Established connections to remote HStoreCoordinators:\n" +
                         StringUtil.join("  ", "\n", HStoreCoordinator.getRemoteCoordinators(catalog_site)));
            }
        });
    }
    
    @Override
    protected HStoreService initHStoreService() {
        return new MockServiceHandler();
    }
    
    private class MockServiceHandler extends HStoreService {

        @Override
        public void transactionInit(RpcController controller, TransactionInitRequest request, RpcCallback<TransactionInitResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            
            TransactionInitWrapperCallback wrapper = new TransactionInitWrapperCallback(getHStoreSite());
            wrapper.init(request.getTransactionId(), request.getPartitionsList(), done);
            txnManager.insert(request.getTransactionId(), request.getPartitionsList(), wrapper, true);
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
        public void shutdown(RpcController controller, ShutdownRequest request, RpcCallback<ShutdownResponse> done) {
            LOG.info("Incoming " + request.getClass().getSimpleName());
            System.exit(0);
        }
    }
    
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
            ArgumentsParser.PARAM_CATALOG
        );
        int site_id = args.getIntOptParam(0);
        
        Site catalog_site = CatalogUtil.getSiteFromId(args.catalog, site_id);
        assert(catalog_site != null) : "Invalid site id #" + site_id;
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args, catalog_site);
        MockCoordinator coordinator = new MockCoordinator(catalog_site);
        HStoreSite hstore_site = coordinator.getHStoreSite();
        coordinator.start(); // Blocks until all connections are established
        
        // Let's try one!
        final LocalTransaction ts = makeLocalTransaction(hstore_site);
        final CountDownLatch latch = new CountDownLatch(1);
        RpcCallback<Hstore.TransactionInitResponse> callback = new RpcCallback<Hstore.TransactionInitResponse>() {
            @Override
            public void run(TransactionInitResponse parameter) {
                LOG.info("GOT CALLBACK FOR " + ts);
                latch.countDown();
            }
        };
        
        coordinator.transactionInit(ts, callback);
        
        // Block until we get our response!
        latch.await();
    }
}
