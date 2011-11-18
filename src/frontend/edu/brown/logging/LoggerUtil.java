package edu.brown.logging;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;
import edu.mit.hstore.HStoreThreadManager;

/**
 * Hack to hook in log4j.properties
 * @author pavlo
 */
public abstract class LoggerUtil {

    private static final String LOG4J_FILENAME = "log4j.properties";
    private static File PROPERTIES_FILE = null;
    private static Thread REFRESH_THREAD = null;
    private static long LAST_TIMESTAMP = 0;
    private static final EventObservable<Object> OBSERVABLE = new EventObservable<Object>();
    private static HStoreThreadManager THREAD_MANAGER;
    
    public static class LoggerBoolean {
        private boolean val;
    
        public LoggerBoolean(boolean val) {
            this.val = val;
        }
        public boolean get() {
            return (this.val);
        }
        public void set(boolean val) {
            this.val = val;
        }
        @Override
        public String toString() {
            return Boolean.toString(this.val);
        }
    }
    
    private static class LoggerObserver extends EventObserver<Object> {
        
        private final Logger logger;
        private final LoggerBoolean debug;
        private final LoggerBoolean trace;
        
        public LoggerObserver(Logger logger, LoggerBoolean debug, LoggerBoolean trace) {
            this.logger = logger;
            this.debug = debug;
            this.trace = trace;
        }
        
        @Override
        public void update(EventObservable<Object> o, Object arg) {
            this.debug.set(this.logger.isDebugEnabled());
            this.trace.set(this.logger.isTraceEnabled());
        }
    }
    
    private static class AtomicObserver extends EventObserver<Object> {
        
        private final Logger logger;
        private final AtomicBoolean debug;
        private final AtomicBoolean trace;
        
        public AtomicObserver(Logger logger, AtomicBoolean debug, AtomicBoolean trace) {
            this.logger = logger;
            this.debug = debug;
            this.trace = trace;
        }
        
        @Override
        public void update(EventObservable<Object> o, Object arg) {
            this.debug.lazySet(this.logger.isDebugEnabled());
            this.trace.lazySet(this.logger.isTraceEnabled());
        }
    }
    
    private static class LoggerCheck implements Runnable {
        final long interval;
        
        public LoggerCheck(long interval) {
            this.interval = interval;
        }
        
        public void run() {
            if (PROPERTIES_FILE == null) setupLogging();
            Thread self = Thread.currentThread();
            self.setName("LogCheck");
            
            while (!self.isInterrupted()) {
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException ex) {
                    break;
                }
                if (THREAD_MANAGER != null) {
                    synchronized (LoggerUtil.class) {
                        THREAD_MANAGER.registerProcessingThread();
                        THREAD_MANAGER = null;
                    } // SYNCH
                }
                
                // Refresh our configuration if the file has changed
                if (PROPERTIES_FILE != null && LAST_TIMESTAMP != PROPERTIES_FILE.lastModified()) {
                    loadConfiguration(PROPERTIES_FILE);
                    assert(PROPERTIES_FILE != null);
                    Logger.getRootLogger().info("Refreshed log4j configuration [" + PROPERTIES_FILE.getAbsolutePath() + "]");
                    LoggerUtil.OBSERVABLE.notifyObservers();
                }
            }
        }
    }
    
    public static synchronized void registerThread(HStoreThreadManager manager) {
        THREAD_MANAGER = manager; 
    }
    
    public static void setupLogging() {
        if (PROPERTIES_FILE != null) return;
        
        // Hack for testing...
        List<String> paths = new ArrayList<String>();
        paths.add(System.getProperty("log4j.configuration", LOG4J_FILENAME));
        
        for (String p : paths) {
            File file = new File(p);
            if (file.exists()) {
                loadConfiguration(file);
                break;
            }
        } // FOR
        // Hack! Load in the root directory one. This is just hack to remove the
        // warning message from FileUtil
        try {
            File findFile = FileUtil.findFile(LOG4J_FILENAME);
            if (findFile != null && findFile.exists()) loadConfiguration(findFile);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        LoggerUtil.refreshLogging(10000); // 180000l); // 3 min
    }
    
    public static Thread getRefreshThread() {
        return (REFRESH_THREAD);
    }
    
    protected static synchronized void loadConfiguration(File file) {
        org.apache.log4j.PropertyConfigurator.configure(file.getAbsolutePath());
        Logger.getRootLogger().debug("Loaded log4j configuration file '" + file.getAbsolutePath() + "'");
        PROPERTIES_FILE = file;
        LAST_TIMESTAMP = file.lastModified();
    }
    
    public static void refreshLogging(final long interval) {
        if (REFRESH_THREAD == null) {
            Logger.getRootLogger().debug("Starting log4j refresh thread [update interval = " + interval + "]");
            REFRESH_THREAD = new Thread(new LoggerCheck(interval));
            REFRESH_THREAD.setPriority(Thread.MIN_PRIORITY);
            REFRESH_THREAD.setDaemon(true);
            REFRESH_THREAD.start();
            
            // We need to update all of our observers the first time
            LoggerUtil.OBSERVABLE.notifyObservers();
        }
    }
    
    
    public static void attachObserver(Logger logger, LoggerBoolean debug, LoggerBoolean trace) {
        LoggerUtil.attachObserver(new LoggerObserver(logger, debug, trace));
    }
    
    public static void attachObserver(Logger logger, AtomicBoolean debug, AtomicBoolean trace) {
        LoggerUtil.attachObserver(new AtomicObserver(logger, debug, trace));
    }
    
    public static void attachObserver(EventObserver<Object> observer) {
        observer.update(null, null);
        LoggerUtil.OBSERVABLE.addObserver(observer);
    }

}
