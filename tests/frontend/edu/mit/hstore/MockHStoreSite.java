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

import edu.brown.catalog.CatalogUtil;
import edu.brown.hstore.Hstore;
import edu.brown.hstore.Hstore.Status;
import edu.brown.hstore.Hstore.TransactionInitResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.markov.TransactionEstimator;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.ThreadUtil;
import edu.mit.hstore.dtxn.LocalTransaction;

public class MockHStoreSite extends HStoreSite {
    private static final Logger LOG = Logger.getLogger(MockHStoreSite.class);
    private final static LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private final static LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    // ----------------------------------------------------------------------------
    // STATIC HELPERS
    // ----------------------------------------------------------------------------
    
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
        Procedure catalog_proc = hstore_site.getDatabase().getProcedures().getIgnoreCase("@NoOp");
        StoredProcedureInvocation invocation = new StoredProcedureInvocation(clientHandle, catalog_proc.getName());
        RpcCallback<byte[]> client_callback = null;
        
        LocalTransaction ts = new LocalTransaction(hstore_site);
        ts.init(txnId, clientHandle, base_partition,
                predict_touchedPartitions, predict_readOnly, predict_canAbort,
                estimator_state, catalog_proc, invocation, client_callback);
        return (ts);
    }
    
    // ----------------------------------------------------------------------------
    // INITIALIZATION
    // ----------------------------------------------------------------------------
    
    public MockHStoreSite(Site catalog_site, PartitionEstimator p_estimator) {
        super(catalog_site,
              makeExecutors(catalog_site, p_estimator),
              p_estimator);
    }
    @Override
    protected HStoreCoordinator initHStoreCoordinator() {
        return new MockHStoreCoordinator(this);
    }
    
    // ----------------------------------------------------------------------------
    // SPECIAL METHOD OVERRIDES
    // ----------------------------------------------------------------------------
    
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

    // ----------------------------------------------------------------------------
    // YE OLD MAIN METHOD
    // ----------------------------------------------------------------------------
    
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs,
            ArgumentsParser.PARAM_CATALOG
        );
        int site_id = args.getIntOptParam(0);
        
        Site catalog_site = CatalogUtil.getSiteFromId(args.catalog, site_id);
        assert(catalog_site != null) : "Invalid site id #" + site_id;
        
        HStoreConf hstore_conf = HStoreConf.initArgumentsParser(args, catalog_site);
        hstore_conf.site.cpu_affinity = false;
        hstore_conf.site.status_interval = -1;
        
        final MockHStoreSite hstore_site = new MockHStoreSite(catalog_site, makePartitionEstimator(catalog_site));
        hstore_site.init().start(); // Blocks until all connections are established
        final MockHStoreCoordinator hstore_coordinator = (MockHStoreCoordinator)hstore_site.getCoordinator();
        assert(hstore_coordinator.isStarted());
        
        final CountDownLatch latch = new CountDownLatch(1);
        
        // Let's try one!
        if (site_id == 0) {
            // Sleep for a few seconds give the other guys time to prepare themselves
            ThreadUtil.sleep(2500);
            
            final LocalTransaction ts = makeLocalTransaction(hstore_site);
            RpcCallback<Hstore.TransactionInitResponse> callback = new RpcCallback<Hstore.TransactionInitResponse>() {
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
