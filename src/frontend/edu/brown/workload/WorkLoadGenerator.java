/***************************************************************************
 *   Copyright (C) 2008 by H-Store Project                                 *
 *   Brown University                                                      *
 *   Massachusetts Institute of Technology                                 *
 *   Yale University                                                       *
 *                                                                         *
 *   Permission is hereby granted, free of charge, to any person obtaining *
 *   a copy of this software and associated documentation files (the       *
 *   "Software"), to deal in the Software without restriction, including   *
 *   without limitation the rights to use, copy, modify, merge, publish,   *
 *   distribute, sublicense, and/or sell copies of the Software, and to    *
 *   permit persons to whom the Software is furnished to do so, subject to *
 *   the following conditions:                                             *
 *                                                                         *
 *   The above copyright notice and this permission notice shall be        *
 *   included in all copies or substantial portions of the Software.       *
 *                                                                         *
 *   THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,       *
 *   EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF    *
 *   MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.*
 *   IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR     *
 *   OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, *
 *   ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR *
 *   OTHER DEALINGS IN THE SOFTWARE.                                       *
 ***************************************************************************/
package edu.brown.workload;

import java.io.File;
import java.util.*;
import org.voltdb.catalog.*;

import edu.brown.catalog.CatalogUtil;

/**
 * 
 * @author pavlo
 */
public class WorkLoadGenerator {
    
    // ----------------------------------------------
    // DEFAULTS
    // ----------------------------------------------
    public static final String DEFAULT_CLUSTER_NAME = "cluster";
    public static final String DEFAULT_DATABASE_NAME = "database";

    /**
     * 
     */
    protected final Database catalog_db;
    
    /**
     * Mapping from StoredProcedure catalog paths to frequencies
     */
    protected final Map<String, Double> frequencies = new HashMap<String, Double>();
    
    public WorkLoadGenerator(Database catalog_db) {
        this.catalog_db = catalog_db;
        
        // Evenly split the frequency of stored procedures
        int num_of_procs = this.catalog_db.getProcedures().size();
        double frequency = 1.0 / num_of_procs;
        for (Procedure proc : this.catalog_db.getProcedures()) {
            this.frequencies.put(proc.getPath(), frequency);
        } // FOR
    }
    
    public Double getFrequency(Procedure proc) {
        return (this.frequencies.get(proc.getPath()));
    }
    public void setFrequency(Procedure proc, Double frequency) {
        this.frequencies.put(proc.getPath(), frequency);
    }
    
    /**
     * Generate a sample trace file using the frequencies
     * @param steps
     * @return
     */
    public String generateTrace(int steps) throws Exception {
        String ret = "";
        
        for (int step = 0; step < steps; step++) {
            //
            // TODO: Randomly decide whether to execute a stored procedure 
            // TODO: Come up with a time scheme of some sort...
            // TODO: Figure out parameters for each stored procedure
            //
        }
        return (ret);
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        
        String jar_path = args[0];
        Integer steps = Integer.parseInt(args[1]);
        
        File file = new File(jar_path);
        if (!file.exists()) {
            System.err.println("ERROR: The project jar file '" + jar_path + "' does not exist");
            System.exit(1);
        }
        Catalog catalog = CatalogUtil.loadCatalogFromJar(file.getPath());
        Database catalog_db = CatalogUtil.getDatabase(catalog);
        WorkLoadGenerator generator = new WorkLoadGenerator(catalog_db);
        try {
            System.out.println(generator.generateTrace(steps));
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
    }

}
