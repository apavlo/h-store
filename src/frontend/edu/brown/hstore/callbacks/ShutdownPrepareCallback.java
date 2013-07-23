package edu.brown.hstore.callbacks;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

import org.apache.log4j.Logger;

import com.google.protobuf.RpcCallback;

import edu.brown.hstore.HStoreCoordinator;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.Hstoreservice.ShutdownPrepareResponse;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class ShutdownPrepareCallback implements RpcCallback<ShutdownPrepareResponse> {
    private static final Logger LOG = Logger.getLogger(HStoreCoordinator.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final Set<Integer> siteids = new HashSet<Integer>();
    private final int num_sites;
    private final CountDownLatch latch;
    
    public ShutdownPrepareCallback(int num_sites, CountDownLatch latch) {
        this.num_sites = num_sites;
        this.latch = latch;
    }
    
    
    @Override
    public void run(ShutdownPrepareResponse parameter) {
        int siteId = parameter.getSenderSite();
        assert(this.siteids.contains(siteId) == false) :
            "Duplicate response from remote HStoreSite " + HStoreThreadManager.formatSiteName(siteId);
        this.siteids.add(siteId);
        if (trace.val) LOG.trace("Received " + this.siteids.size() + "/" + num_sites + " shutdown acknowledgements");
        this.latch.countDown();
    }
};