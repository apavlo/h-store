package edu.brown.clusterreorganizer;

import org.apache.log4j.Logger;

import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.hstore.conf.HStoreConf;
import edu.brown.hstore.interfaces.Shutdownable;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;

/**
 * 
 * @author yang yang@cs.brown.edu
 *
 */

public class ClusterReorganizer implements Runnable, Shutdownable {
    public static final Logger LOG = Logger.getLogger(HStore.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private final HStoreSite hstore_site;
    private final HStoreConf hstore_conf;
    private Thread self;
    private final int interval; // milliseconds
    
    public ClusterReorganizer(HStoreSite hstore_site){
        this.hstore_site = hstore_site;
        this.hstore_conf = hstore_site.getHStoreConf();
        this.interval = hstore_conf.site.status_interval;
        
        //Print something where HStore_site is read to run
        this.hstore_site.getReadyObservable().addObserver(new EventObserver<Object>() {
            @Override
            public void update(EventObservable<Object> arg0, Object arg1) {
//                if (debug.get())
                    LOG.info("A blank HStore_site is ready to run !!! --Live Migration");
            }
        });
    }
    
    @Override
    public void run() {
        self = Thread.currentThread();
        self.setName(HStoreThreadManager.getThreadName(hstore_site, "ClusterReorganizer"));
        self.setName(HStoreThreadManager.getThreadName(hstore_site, "mon"));
        if (hstore_conf.site.cpu_affinity)
            hstore_site.getThreadManager().registerProcessingThread();
        if (LOG.isDebugEnabled()) LOG.debug("Cluster Reorganizer for Live Migration is running now !!");
        
        while(!this.self.isInterrupted()){
            try {
                Thread.sleep(this.interval);
                LOG.info("Cluster Reorganizer is still running");
            } catch (InterruptedException ex) {
                return;
            }
        }
    }

    @Override
    public void prepareShutdown(boolean error) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public void shutdown() {
        if (debug.get())
            LOG.debug(String.format("Cluster Reorganizer is shutting down!!!"));
        if (this.self != null)
            this.self.interrupt(); 
    }

    @Override
    public boolean isShuttingDown() {
        return this.hstore_site.isShuttingDown();
    }
    
}
