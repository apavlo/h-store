package edu.brown.hstore;

import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.ClientResponseImpl;
import org.voltdb.ParameterSet;
import org.voltdb.catalog.Procedure;
import org.voltdb.utils.EstTime;
import org.voltdb.utils.EstTimeUpdater;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.Hstoreservice.Status;
import edu.brown.hstore.Hstoreservice.TransactionInitResponse;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.hstore.txns.RemoteTransaction;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionSet;
import edu.brown.utils.ThreadUtil;

public class MockHStoreSite extends HStoreSite {
    private static final Logger LOG = Logger.getLogger(MockHStoreSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean();
    private final static LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // STATIC HELPERS
    // ----------------------------------------------------------------------------
    
    static LocalTransaction makeLocalTransaction(HStoreSite hstore_site) {
        long txnId = hstore_site.getTransactionIdManager(0).getNextUniqueTransactionId();
        long clientHandle = -1;
        
        CatalogContext catalogContext = hstore_site.getCatalogContext();
        int base_partition = CollectionUtil.random(hstore_site.getLocalPartitionIds());
        PartitionSet predict_touchedPartitions = catalogContext.getAllPartitionIds();
        boolean predict_readOnly = false;
        boolean predict_canAbort = true;
        Procedure catalog_proc = catalogContext.procedures.getIgnoreCase("@NoOp");
        ParameterSet params = new ParameterSet();
        RpcCallback<ClientResponseImpl> client_callback = null;
        
        LocalTransaction ts = new LocalTransaction(hstore_site);
        ts.init(txnId, EstTime.currentTimeMillis(), clientHandle, base_partition,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                catalog_proc, params, client_callback);
        EstTimeUpdater.update(System.currentTimeMillis());
        return (ts);
    }
    
    static RemoteTransaction makeDistributedTransaction(HStoreSite base_hstore_site, HStoreSite remote_hstore_site) {
        long txnId = base_hstore_site.getTransactionIdManager(0).getNextUniqueTransactionId();
        long clientHandle = -1;
        
        CatalogContext catalogContext = base_hstore_site.getCatalogContext();
        int base_partition = CollectionUtil.random(base_hstore_site.getLocalPartitionIds());
        PartitionSet predict_touchedPartitions = catalogContext.getAllPartitionIds();
        int partition = CollectionUtil.random(remote_hstore_site.getLocalPartitionIds());
        predict_touchedPartitions.add(partition);
        boolean predict_readOnly = false;
        boolean predict_canAbort = true;
        Procedure catalog_proc = catalogContext.procedures.getIgnoreCase("@NoOp");
        ParameterSet params = new ParameterSet();
        RpcCallback<ClientResponseImpl> client_callback = null;
        
        LocalTransaction ts = new LocalTransaction(base_hstore_site);
        ts.init(txnId, EstTime.currentTimeMillis(), clientHandle, base_partition,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                catalog_proc, params, client_callback);
        
        base_hstore_site.getTransactionInitializer().registerTransaction(ts, base_partition);
        PartitionSet partitions = remote_hstore_site.getLocalPartitionIds();
        RemoteTransaction remote_ts = remote_hstore_site.getTransactionInitializer().createRemoteTransaction(txnId, partitions, params, base_partition, catalog_proc.getId()); 
        
        EstTimeUpdater.update(System.currentTimeMillis());
        return (remote_ts);
    }

	private HStoreCoordinator hstore_coordinator;
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    /**
     * Create an new MockHStoreSite for testing.
     * Note that this will not start any of the internal resources
     * Use MockHStoreSite.init() to do that!
     * @param site_id TODO
     * @param catalogContext
     * @param hstore_conf
     */
    public MockHStoreSite(int site_id, CatalogContext catalogContext, HStoreConf hstore_conf) {
        super(site_id, catalogContext, hstore_conf);
        
        hstore_conf.site.status_enable = false;
        
        for (int p : this.getLocalPartitionIds().values()) {
            MockPartitionExecutor executor = new MockPartitionExecutor(p, catalogContext,
                                                                       this.getPartitionEstimator());
            this.addPartitionExecutor(p, executor);
        } // FOR
    }
    
    public void setCoordinator(){
    	this.hstore_coordinator = this.initHStoreCoordinator();
    }
    
    @Override
    public AntiCacheManager getAntiCacheManager(){
    	return new MockAntiCacheManager(this); 
    }
    
    @Override
    public HStoreCoordinator getCoordinator(){
    	return this.hstore_coordinator;
    }
    
    @Override
    public HStoreCoordinator initHStoreCoordinator() {
        return new MockHStoreCoordinator(this);
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL METHOD OVERRIDES
    // ----------------------------------------------------------------------------
    
    @Override
    public Status transactionRestart(LocalTransaction orig_ts, Status status) {
        int restart_limit = 10;
        if (orig_ts.getRestartCounter() > restart_limit) {
            if (orig_ts.isSysProc()) {
                throw new RuntimeException(String.format("%s has been restarted %d times! Rejecting...",
                                           orig_ts, orig_ts.getRestartCounter()));
            } else {
                this.transactionReject(orig_ts, Status.ABORT_REJECT);
                return (Status.ABORT_REJECT);
            }
        }
        return (status);
    }

    // ----------------------------------------------------------------------------
    // YE OLD MAIN METHOD
    // ----------------------------------------------------------------------------
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
            ArgumentsParser.PARAM_CATALOG
        );
        int site_id = args.getIntOptParam(0);
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args);
        hstore_conf.site.cpu_affinity = false;
        hstore_conf.site.status_enable = false;
        
        final MockHStoreSite hstore_site = new MockHStoreSite(site_id, args.catalogContext, hstore_conf);
        hstore_site.init().run(); // Blocks until all connections are established
        final MockHStoreCoordinator hstore_coordinator = (MockHStoreCoordinator)hstore_site.getCoordinator();
        assert(hstore_coordinator.isStarted());
        
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Let's try one!
        if (site_id == 0) {
            // Sleep for a few seconds give the other guys time to prepare themselves
            ThreadUtil.sleep(2500);
            
            final LocalTransaction ts = makeLocalTransaction(hstore_site);
            RpcCallback<TransactionInitResponse> callback = new RpcCallback<TransactionInitResponse>() {
                @Override
                public void run(TransactionInitResponse parameter) {
                    LOG.info("GOT CALLBACK FOR " + ts);
                    latch.countDown();
                }
            };
            LOG.info("Sending init for " + ts);
            hstore_coordinator.transactionInit(ts, callback);
        }
        
        // Block until we get our response!
        latch.await();
    }
}
