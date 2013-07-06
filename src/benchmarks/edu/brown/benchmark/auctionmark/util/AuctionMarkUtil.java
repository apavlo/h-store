/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
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
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public static File getDataDirectory() {
        File dataDir = null;
        
        // If we weren't given a path, then we need to look for the tests directory and
        // then walk our way up the tree to get to our benchmark's directory
        try {
            File tests_dir = FileUtil.findDirectory("src");
            assert(tests_dir != null);
            
            dataDir = new File(tests_dir.getAbsolutePath() + File.separator + "benchmarks" + File.separator +
                               AuctionMarkProfile.class.getPackage().getName().replace('.', File.separatorChar) +
                               File.separator + "data").getCanonicalFile();
            if (debug.val) LOG.debug("Default data directory path = " + dataDir);
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
