package edu.brown.utils;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.log4j.Logger;

/**
 * Hack to hook in log4j.properties
 * @author pavlo
 */
public abstract class LoggerUtil {

    private static final String log4j_filename = "log4j.properties";
    private static File log4j_properties_file = null;
    private static Thread refresh_thread = null;
    private static long last_timestamp = 0;
    private static final EventObservable observable = new EventObservable();
    
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
    }
    
    private static class LoggerObserver extends EventObserver {
        
        private final Logger logger;
        private final LoggerBoolean debug;
        private final LoggerBoolean trace;
        
        public LoggerObserver(Logger logger, LoggerBoolean debug, LoggerBoolean trace) {
            this.logger = logger;
            this.debug = debug;
            this.trace = trace;
        }
        
        @Override
        public void update(Observable o, Object arg) {
            this.debug.set(this.logger.isDebugEnabled());
            this.trace.set(this.logger.isTraceEnabled());
        }
    }
    
    private static class AtomicObserver extends EventObserver {
        
        private final Logger logger;
        private final AtomicBoolean debug;
        private final AtomicBoolean trace;
        
        public AtomicObserver(Logger logger, AtomicBoolean debug, AtomicBoolean trace) {
            this.logger = logger;
            this.debug = debug;
            this.trace = trace;
        }
        
        @Override
        public void update(Observable o, Object arg) {
            this.debug.lazySet(this.logger.isDebugEnabled());
            this.trace.lazySet(this.logger.isTraceEnabled());
        }
    }
    
    public static void setupLogging() {
        if (log4j_properties_file != null) return;
        
        // Hack for testing...
        List<String> paths = new ArrayList<String>();
        paths.add(System.getProperty("log4j.configuration", log4j_filename));
        
        // These probably aren't needed anymore
        paths.add("/home/pavlo/Documents/H-Store/SVN-Brown/trunk/" + log4j_filename);
        paths.add("/home/pavlo/Documents/H-Store/SVN-Brown/branches/markov-branch/" + log4j_filename);
        paths.add("/host/work/hstore/src/" + log4j_filename);
        paths.add("/research/hstore/sw47/trunk/" + log4j_filename);
        
        
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
            File findFile = FileUtil.findFile(log4j_filename);
            if (findFile != null && findFile.exists()) loadConfiguration(findFile);
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        LoggerUtil.refreshLogging(10000); // 180000l); // 3 min
    }
    
    protected static void loadConfiguration(File file) {
        org.apache.log4j.PropertyConfigurator.configure(file.getAbsolutePath());
        Logger.getRootLogger().debug("Loaded log4j configuration file '" + file.getAbsolutePath() + "'");
        log4j_properties_file = file;
        last_timestamp = file.lastModified();
    }
    
    public static void refreshLogging(final long interval) {
        if (refresh_thread == null) {
            Logger.getRootLogger().debug("Starting log4j refresh thread [update interval = " + interval + "]");
            refresh_thread = new Thread() {
                public void run() {
                    if (log4j_properties_file == null) setupLogging();
                    Thread self = Thread.currentThread();
                    self.setName("LogCheck");
                    while (!self.isInterrupted()) {
                        try {
                            Thread.sleep(interval);
                        } catch (InterruptedException ex) {
                            break;
                        }
                        // Refresh our configuration if the file has changed
                        if (log4j_properties_file != null && last_timestamp != log4j_properties_file.lastModified()) {
                            loadConfiguration(log4j_properties_file);
                            Logger.getRootLogger().info("Refreshed log4j configuration [" + log4j_properties_file.getAbsolutePath() + "]");
                            LoggerUtil.observable.notifyObservers();
                        }
                    }
                }
            };
            refresh_thread.setDaemon(true);
            refresh_thread.start();
            
            // We need to update all of our observers the first time
            LoggerUtil.observable.notifyObservers();
        }
    }
    
    
    public static void attachObserver(Logger logger, LoggerBoolean debug, LoggerBoolean trace) {
        LoggerUtil.attachObserver(new LoggerObserver(logger, debug, trace));
    }
    
    public static void attachObserver(Logger logger, AtomicBoolean debug, AtomicBoolean trace) {
        LoggerUtil.attachObserver(new AtomicObserver(logger, debug, trace));
    }
    
    public static void attachObserver(EventObserver observer) {
        LoggerUtil.observable.addObserver(observer);
    }
}
