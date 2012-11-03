package edu.brown.logging;

import java.io.File;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import edu.brown.hstore.HStore;
import edu.brown.hstore.HStoreConstants;
import edu.brown.hstore.HStoreSite;
import edu.brown.hstore.HStoreThreadManager;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.EventObservable;
import edu.brown.utils.EventObserver;
import edu.brown.utils.FileUtil;

/**
 * Hack to hook in log4j.properties
 * @author pavlo
 */
public abstract class LoggerUtil {

    private static final String LOG4J_FILENAME = "log4j.properties";
    private static File PROPERTIES_FILE = null;
    private static LoggerCheck REFRESH_CHECKER = null;
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
            if (this.debug != null) this.debug.set(this.logger.isDebugEnabled());
            if (this.trace != null) this.trace.set(this.logger.isTraceEnabled());
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
        private long interval;
        
        public LoggerCheck(long interval) {
            this.interval = interval;
        }
        
        public void run() {
            if (PROPERTIES_FILE == null) setupLogging();
            Thread self = Thread.currentThread();
            self.setName(HStoreConstants.THREAD_NAME_LOGGING);
            
            while (!self.isInterrupted()) {
                try {
                    Thread.sleep(this.interval);
                } catch (InterruptedException ex) {
                    break;
                }
                
                // HACK: Look for an HStoreSite so that we can set our name properly
                // This probably doesn't need to be synchronized
                if (THREAD_MANAGER == null) {
                    synchronized (LoggerUtil.class) {
                        if (THREAD_MANAGER == null) {
                            HStoreSite hstore_site = HStore.instance();
                            if (hstore_site != null) {
                                String name = HStoreThreadManager.getThreadName(hstore_site, HStoreConstants.THREAD_NAME_LOGGING);
                                self.setName(name);
                                THREAD_MANAGER = hstore_site.getThreadManager();
                                THREAD_MANAGER.registerProcessingThread();
                            }
                        }
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
    
    public static synchronized void setupLogging() {
        if (PROPERTIES_FILE != null) return;
        
        // Hack for testing...
        List<String> paths = new ArrayList<String>();
        String log4jPath = System.getProperty("log4j.configuration", LOG4J_FILENAME); 
        paths.add(log4jPath);
//        System.err.println(log4jPath + " -> " + FileUtil.exists(log4jPath));
        
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

        LoggerUtil.refreshLogging(30000); // 180000l); // 3 min
    }
    
    protected static synchronized void loadConfiguration(File file) {
        org.apache.log4j.PropertyConfigurator.configure(file.getAbsolutePath());
        Logger.getRootLogger().debug("Loaded log4j configuration file '" + file.getAbsolutePath() + "'");
        PROPERTIES_FILE = file;
        LAST_TIMESTAMP = file.lastModified();
    }
    
    public static synchronized void refreshLogging(final long interval) {
        if (REFRESH_THREAD == null) {
            Logger.getRootLogger().debug("Starting log4j refresh thread [update interval = " + interval + "]");
            REFRESH_CHECKER = new LoggerCheck(interval);
            REFRESH_THREAD = new Thread(REFRESH_CHECKER);
            REFRESH_THREAD.setPriority(Thread.MIN_PRIORITY);
            REFRESH_THREAD.setDaemon(true);
            REFRESH_THREAD.start();
            
            // We need to update all of our observers the first time
            LoggerUtil.OBSERVABLE.notifyObservers();
        } else if (interval != REFRESH_CHECKER.interval) {
            REFRESH_CHECKER.interval = interval;
        }
    }
    
    /**
     * From http://stackoverflow.com/a/3187802/42171
     */
    @SuppressWarnings("unchecked")
    public static void flushAllLogs() {
        try {
            Set<FileAppender> flushedFileAppenders = new HashSet<FileAppender>();
            for (Object nextLogger : CollectionUtil.iterable(LogManager.getLoggerRepository().getCurrentLoggers())) {
                if (nextLogger instanceof Logger) {
                    Logger currentLogger = (Logger) nextLogger;
                    Enumeration allAppenders = currentLogger.getAllAppenders();
                    while (allAppenders.hasMoreElements()) {
                        Object nextElement = allAppenders.nextElement();
                        if (nextElement instanceof FileAppender) {
                            FileAppender fileAppender = (FileAppender) nextElement;
                            if (!flushedFileAppenders.contains(fileAppender) && !fileAppender.getImmediateFlush()) {
                                flushedFileAppenders.add(fileAppender);
                                // log.info("Appender "+fileAppender.getName()+" is not doing immediateFlush ");
                                fileAppender.setImmediateFlush(true);
                                currentLogger.info("FLUSH");
                                fileAppender.setImmediateFlush(false);
                            } else {
                                // log.info("fileAppender"+fileAppender.getName()+" is doing immediateFlush");
                            }
                        }
                    }
                }
            }
        } catch (RuntimeException e) {
            Logger root = Logger.getRootLogger();
            root.error("Failed flushing logs", e);
        }
    }
    
    public static void attachObserver(Logger logger, LoggerBoolean debug, LoggerBoolean trace) {
        LoggerUtil.attachObserver(new LoggerObserver(logger, debug, trace));
    }
    
    public static void attachObserver(Logger logger, LoggerBoolean debug) {
        LoggerUtil.attachObserver(new LoggerObserver(logger, debug, null));
    }
    
    public static void attachObserver(Logger logger, AtomicBoolean debug, AtomicBoolean trace) {
        LoggerUtil.attachObserver(new AtomicObserver(logger, debug, trace));
    }
    
    public static void attachObserver(EventObserver<Object> observer) {
        observer.update(null, null);
        LoggerUtil.OBSERVABLE.addObserver(observer);
    }

}
