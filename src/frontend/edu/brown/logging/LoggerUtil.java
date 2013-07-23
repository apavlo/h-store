package edu.brown.logging;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggerRepository;

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
    
    /**
     * Simple boolean object used to determine whether to output a log4j message.
     * When this object is attached to the LoggerUtil observerable, it will automatically
     * get updated when its corresponding logger's debug level changes.
     * I did this so that I didn't have to call LOG.isDebugEnabled() all over the place.
     * @author pavlo
     */
    public static class LoggerBoolean {
        /**
         * Whether the log output tracked by this object is set to enabled.
         * This will be updated automatically if this LoggerBoolean is attached
         * to the LoggerObserver managed by the LoggerUtil.
         */
        public boolean val;
    
        public LoggerBoolean() {
            this(false);
        }
        @Deprecated
        public LoggerBoolean(boolean val) {
            this.val = val;
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
            
            if (this.debug != null) this.debug.set(logger.isDebugEnabled());
            if (this.trace != null) this.trace.set(logger.isTraceEnabled());
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
     * Flush the appenders for all of the active loggers
     */
    public static void flushAllLogs() {
        LoggerRepository loggerRepo = LogManager.getLoggerRepository();
        for (Logger logger : CollectionUtil.iterable(loggerRepo.getCurrentLoggers(), Logger.class)) {
            LoggerUtil.flushLogs(logger);
        } // FOR
    }
    
    /**
     * From http://stackoverflow.com/a/3187802/42171
     */
    public static void flushLogs(Logger logger) {
        Logger root = Logger.getRootLogger();
        Set<FileAppender> flushed = new HashSet<FileAppender>();
        try {
            for (Appender appender : CollectionUtil.iterable(logger.getAllAppenders(), Appender.class)) {
                if (appender instanceof FileAppender) {
                    FileAppender fileAppender = (FileAppender)appender;
                    synchronized (fileAppender) {
                        if (!flushed.contains(fileAppender) && !fileAppender.getImmediateFlush()) {
                            root.info(String.format("Appender %s.%s is not doing an immediateFlush",
                                      logger.getName(), appender.getName()));
                            fileAppender.setImmediateFlush(true);
                            logger.info("FLUSH");
                            fileAppender.setImmediateFlush(false);
                            flushed.add(fileAppender);
                        } else {
                            root.info(String.format("Appender %s.%s is doing an immediateFlush",
                                      logger.getName(), appender.getName()));
                        }
                    } // SYNCH
                } else {
                    root.debug(String.format("Unable to flush non-file appender %s.%s",
                               logger.getName(), appender.getName()));
                }
            } // FOR (appender)
        } catch (Throwable ex) {
            root.error("Failed flushing logs for " + logger, ex);
        }
    }
    
    /**
     * Add the LoggerBooleans to be automatically updated by the LoggerUtil thread.
     * @param logger
     * @param debug
     * @param trace
     */
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
