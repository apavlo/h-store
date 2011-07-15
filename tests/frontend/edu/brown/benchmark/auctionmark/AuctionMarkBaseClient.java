/***************************************************************************
 *  Copyright (C) 2010 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
 *                                                                         *
 *  Visawee Angkanawaraphan (visawee@cs.brown.edu)                         *
 *  http://www.cs.brown.edu/~visawee/                                      *
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
package edu.brown.benchmark.auctionmark;

import java.io.File;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.voltdb.catalog.*;

import edu.brown.benchmark.BenchmarkComponent;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.rand.DefaultRandomGenerator;
import edu.brown.utils.FileUtil;

/**
 *
 */
public abstract class AuctionMarkBaseClient extends BenchmarkComponent {
    protected final Logger LOG;
    
    /**
     * Benchmark Profile
     */
    protected final AuctionMarkBenchmarkProfile profile;
    protected final File profile_path;
    
    /**
     * Specialized random number generator
     */
    protected final AbstractRandomGenerator rng;
    
    /**
     * Base catalog objects that we can reference to figure out how to access Volt
     */
    protected final Catalog catalog;
    protected final Database catalog_db;

    /**
     * Whether to enable debug information
     */
    protected boolean debug;
    
    /**
     * Path to directory with data files needed by the loader 
     */
    protected final String data_directory;
    
    /**
     * @param args
     */
    public AuctionMarkBaseClient(Class<? extends AuctionMarkBaseClient> child_class, String[] args) {
        super(args);
        LOG = Logger.getLogger(child_class);
        this.debug = LOG.isDebugEnabled();
        
        Double scale_factor = 1.0;
        String profile_file = null;
        int seed = 0;
        String randGenClassName = DefaultRandomGenerator.class.getName();
        String randGenProfilePath = null;
        String dataDir = null;
        
        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Scale Factor
            if (key.equalsIgnoreCase("SCALEFACTOR")) {
                scale_factor = Double.parseDouble(value);
            // Benchmark Profile File
            } else if (key.equalsIgnoreCase("BENCHMARKPROFILE")) {
                profile_file = value;
            // Random Generator Seed
            } else if (key.equalsIgnoreCase("RANDOMSEED")) {
                seed = Integer.parseInt(value);
            // Random Generator Class
            } else if (key.equalsIgnoreCase("RANDOMGENERATOR")) {
                randGenClassName = value;
            // Random Generator Profile File
            } else if (key.equalsIgnoreCase("RANDOMPROFILE")) {
                randGenProfilePath = value;
            // Data directory
            } else if (key.equalsIgnoreCase("DATADIR")) {
                dataDir = value;
            }
        } // FOR
        assert(scale_factor != null);
        
        // BenchmarkProfile
        // Only load from the file for AuctionMarkClient
        this.profile = new AuctionMarkBenchmarkProfile();
        if (child_class.equals(AuctionMarkClient.class)) {
            this.profile_path = new File(profile_file);
            if (this.profile_path.exists()) {
                try {
                    LOG.info("Loading Profile: " + this.profile_path.getAbsolutePath());
                    this.profile.load(this.profile_path.getAbsolutePath(), null);
                } catch (Exception ex) {
                    LOG.error("Failed to load benchmark profile file '" + this.profile_path + "'", ex);
                    System.exit(1);
                }
            }
        } else {
            this.profile_path = null;
        }
        if (scale_factor != null) this.profile.setScaleFactor(scale_factor);
        
        // Data Directory Path
        if (dataDir == null) {
            // If we weren't given a path, then we need to look for the tests directory and
            // then walk our way up the tree to get to our benchmark's directory
            try {
                File tests_dir = FileUtil.findDirectory("tests");
                assert(tests_dir != null);
                
                File path = new File(tests_dir.getAbsolutePath() + File.separator + "frontend" + File.separator +
                                     AuctionMarkBaseClient.class.getPackage().getName().replace('.', File.separatorChar) +
                                     File.separator + "data").getCanonicalFile();
                if (this.debug) LOG.debug("Default data directory path = " + path);
                if (!path.exists()) {
                    throw new RuntimeException("The default data directory " + path + " does not exist");
                } else if (!path.isDirectory()) {
                    throw new RuntimeException("The default data path " + path + " is not a directory");
                }
                dataDir = path.getAbsolutePath();
            } catch (IOException ex) {
                LOG.fatal("Unexpected error", ex);
            }
        }
        this.data_directory = dataDir;
        if (this.data_directory == null) LOG.warn("No data directory was set!");
        else LOG.debug("AuctionMark Data Directory: " + dataDir);
        
        // Random Generator
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        this.rng = rng;
        
        // Catalog
        Catalog _catalog = null;
        try {
            _catalog = this.getCatalog();
        } catch (Exception ex) {
            LOG.error("Failed to retrieve already compiled catalog", ex);
            System.exit(1);
        }
        this.catalog = _catalog;
        this.catalog_db = CatalogUtil.getDatabase(this.catalog);
    }
    
    /**
     * Save the information stored in the BenchmarkProfile out to a file 
     * @throws IOException
     */
    public File saveProfile() {
        assert(this.profile != null);
        File f = FileUtil.getTempFile("profile", false);
        if (this.debug) LOG.debug("Saving BenchmarkProfile to '" + f + "'");
        try {
            this.profile.save(f.getAbsolutePath());
        } catch (IOException ex) {
            LOG.fatal("Failed to save BenchmarkProfile", ex);
            System.exit(1);
        }
        try {
            for (int i = 0; i < this.getNumClients(); i++) {
                this.sendFileToClient(i, "BENCHMARKPROFILE", f);
            } // FOR
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return (f);
    }

    /**
     * Returns the catalog object for a Table
     * @param tableName
     * @return
     */
    protected Table getTableCatalog(String tableName) {
        return (this.catalog_db.getTables().get(tableName));
    }
}
