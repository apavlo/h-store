package edu.brown.benchmark.auctionmark.util;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;

import edu.brown.benchmark.auctionmark.AuctionMarkProfile;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.FileUtil;

public abstract class AuctionMarkUtil {
    private static final Logger LOG = Logger.getLogger(AuctionMarkUtil.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static File getDataDirectory() {
        File dataDir = null;
        
        // If we weren't given a path, then we need to look for the tests directory and
        // then walk our way up the tree to get to our benchmark's directory
        try {
            File tests_dir = FileUtil.findDirectory("tests");
            assert(tests_dir != null);
            
            dataDir = new File(tests_dir.getAbsolutePath() + File.separator + "frontend" + File.separator +
                               AuctionMarkProfile.class.getPackage().getName().replace('.', File.separatorChar) +
                               File.separator + "data").getCanonicalFile();
            if (debug.get()) LOG.debug("Default data directory path = " + dataDir);
            if (!dataDir.exists()) {
                throw new RuntimeException("The default data directory " + dataDir + " does not exist");
            } else if (!dataDir.isDirectory()) {
                throw new RuntimeException("The default data path " + dataDir + " is not a directory");
            }
        } catch (IOException ex) {
            throw new RuntimeException("Unexpected error", ex);
        }
        return (dataDir);
    }
    
}
