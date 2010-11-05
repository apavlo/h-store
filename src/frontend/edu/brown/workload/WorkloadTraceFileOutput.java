/***************************************************************************
 *   Copyright (C) 2009 by H-Store Project                                 *
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

import java.io.*;
import java.util.*;
import org.apache.log4j.Logger;
import java.util.zip.GZIPInputStream;

import org.json.*;

import org.voltdb.catalog.*;

import edu.brown.statistics.*;
import edu.brown.utils.*;
import edu.brown.workload.AbstractWorkload.Filter.FilterResult;

/**
 * 
 * @author Andy Pavlo <pavlo@cs.brown.edu>
 *
 */
public class WorkloadTraceFileOutput extends AbstractWorkload {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(WorkloadTraceFileOutput.class.getName());
        
    //
    // The output stream that we're going to write our traces to
    //
    private FileOutputStream out;

    //
    // Handle -> Database
    //
    private final Map<TransactionTrace, Database> xact_db_xref = new HashMap<TransactionTrace, Database>();
    
    //
    // The last file that we loaded from
    //
    private File input_path;
    private File output_path;
    
    //
    // Stats Path
    //
    protected String stats_output;
    protected boolean saved_stats = false;
    
    /**
     * Default Constructor
     */
    public WorkloadTraceFileOutput() {
        super();
    }
    
    /**
     * Convenience construct to load in the catalog
     * @param catalog
     */
    public WorkloadTraceFileOutput(Catalog catalog) {
        super(catalog);
    }
    
    /**
     * We want to dump out index structures and other things to the output file
     * when we are going down.
     */
    @Override
    protected void finalize() throws Throwable {
        if (this.out != null) {
            System.err.println("Flushing workload trace output and closing files...");
            
            //
            // Workload Statistics
            //
            if (this.stats != null) this.saveStats();
            //
            // This was here in case I wanted to dump out auxillary data structures
            // As of right now, I don't need to do that, so we'll just flush and close the file
            //
            this.out.flush();
            this.out.close();
        }
        super.finalize();
    }
    
    /**
     * 
     */
    protected void validate() {
        LOG.debug("Checking to make sure there are no duplicate trace objects in workload");
        Set<Long> trace_ids = new HashSet<Long>();
        for (AbstractTraceElement<?> element : this) {
            long trace_id = element.getId();
            assert(!trace_ids.contains(trace_id)) : "Duplicate Trace Element: " + element;
            trace_ids.add(trace_id);
        } // FOR
    }
    
    /**
     * 
     */
    @Override
    public void setOutputPath(String path) {
        this.output_path = new File(path);
        try {
            this.out = new FileOutputStream(path);
            LOG.debug("Opened file '" + path + "' for logging workload trace");
        } catch (Exception ex) {
            LOG.fatal("Failed to open trace output file: " + path);
            ex.printStackTrace();
            System.exit(1);
        }
    }
    
    public void setStatsOutputPath(String path) {
        this.stats_output = path;
    }
    
    /**
     * 
     * @throws Exception
     */
    private void saveStats() throws Exception {
        for (TableStatistics table_stats : this.stats.getTableStatistics()) {
            table_stats.postprocess(this.stats_catalog_db);
        } // FOR
        if (this.stats_output != null) {
            this.stats.save(this.stats_output);
        }
    }
    
    /**
     * 
     * @param input_path
     * @param catalog_db
     * @throws Exception
     */
    public void load(String input_path, Database catalog_db) throws Exception {
        this.load(input_path, catalog_db, null);
    }
    
    /**
     * 
     * @param input_path
     * @param catalog_db
     * @param limit
     * @throws Exception
     */
    public void load(String input_path, Database catalog_db, Filter filter) throws Exception {
        final boolean trace = LOG.isTraceEnabled();
        final boolean debug = LOG.isDebugEnabled();
        
        if (debug) LOG.debug("Reading workload trace from file '" + input_path + "'");
        File file = new File(input_path);

        // Check whether it's gzipped. Yeah that's right, we support that!
        BufferedReader in = null;
        if (file.getPath().endsWith(".gz")) {
            FileInputStream fin = new FileInputStream(file);
            GZIPInputStream gzis = new GZIPInputStream(fin);
            in = new BufferedReader(new InputStreamReader(gzis));
        } else {
            in = new BufferedReader(new FileReader(file));   
        }
        
        long xact_ctr = 0;
        long query_ctr = 0;
        long line_ctr = 0;
        long element_ctr = 0;
        
        while (in.ready()) {
//            StringBuilder buffer = new StringBuilder();
//            do {
//                String line = in.readLine();
//                buffer.append(line);
//                if (line.equals("}")) break;
//            } while (in.ready());
//            JSONObject jsonObject = new JSONObject(buffer.toString()); //in.readLine());
            String line = in.readLine().trim();
            if (line.isEmpty()) continue;
            JSONObject jsonObject = null; 
            try {
                jsonObject = new JSONObject(line);
            } catch (Exception ex) {
                throw new Exception("Error on line " + (line_ctr+1) + " of workload trace file '" + file.getName() + "'", ex);
            }
            
            //
            // TransactionTrace
            //
            if (jsonObject.has(TransactionTrace.Members.XACT_ID.name())) {
                // If we have already loaded in up to our limit, then we don't need to
                // do anything else. But we still have to keep reading because we need
                // be able to load in our index structures that are at the bottom of the file
                //
                // NOTE: If we ever load something else but the straight trace dumps, then the following
                // line should be a continue and not a break.
                
                // Load the xact from the jsonObject
                TransactionTrace xact = null;
                try {
                    xact = TransactionTrace.loadFromJSONObject(jsonObject, catalog_db);
                } catch (Exception ex) {
                    LOG.error("Failed JSONObject:\n" + jsonObject.toString(2));
                    throw ex;
                }
                if (xact == null) {
                    throw new Exception("Failed to deserialize transaction trace on line " + xact_ctr);
                } else if (filter != null) {
                    FilterResult result = filter.apply(xact);
                    if (result == FilterResult.HALT) break;
                    else if (result == FilterResult.SKIP) continue;
                    if (trace) LOG.trace(result + ": " + xact);
                }

                // Keep track of how many trace elements we've loaded so that we can make sure
                // that our element trace list is complete
                xact_ctr++;
                query_ctr += xact.getQueryCount();
                if (trace && xact_ctr % 10000 == 0) LOG.trace("Read in " + xact_ctr + " transactions...");
                element_ctr += 1 + xact.getQueries().size();
                
                // This call just updates the various other index structures 
                this.addTransaction(xact.getCatalogItem(catalog_db), xact);
                
            // Unknown!
            } else {
                LOG.fatal("Unexpected serialization line in workload trace '" + input_path + "' on line " + line_ctr);
                System.exit(1);
            }
            line_ctr++;
        } // WHILE
        in.close();
        this.validate();
        LOG.info("Loaded in " + this.xact_trace.size() + " transactions with a total of " + query_ctr + " queries from workload trace '" + file.getName() + "'");
        this.input_path = new File(input_path);
        return;
    }
    
    public void save(String path, Database catalog_db) {
        this.setOutputPath(path);
        this.save(catalog_db);
    }
    
    public void save(Database catalog_db) {
        LOG.info("Writing out workload trace to '" + this.output_path + "'");
        for (AbstractTraceElement<?> element : this) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                try {
                    //String json = xact.toJSONString(catalog_db);
                    //JSONObject jsonObject = new JSONObject(json);
                    //this.out.write(jsonObject.toString(2).getBytes());
                    
                    this.out.write(xact.toJSONString(catalog_db).getBytes());
                    this.out.write("\n".getBytes());
                    this.out.flush();
                    LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
                } catch (Exception ex) {
                    LOG.fatal(ex.getMessage());
                    ex.printStackTrace();
                }
            }
        } // FOR
    }
    
    /**
     * 
     */
    @Override
    public TransactionTrace startTransaction(Object caller, final Procedure catalog_proc, Object[] args) {
        TransactionTrace handle = super.startTransaction(caller, catalog_proc, args);
        if (handle != null) {
            this.xact_db_xref.put(handle, (Database)catalog_proc.getParent());
            // If this is the first non bulk-loader proc that we have seen, then
            // go ahead and save the stats out to a file in case we crash later on
            if (!this.saved_stats && this.stats != null) {
                try {
                    this.saveStats();
                    this.saved_stats = true;
                } catch (Exception ex) {
                    ex.printStackTrace();
                    System.exit(1);
                }
            }
        }
        return (handle);
    }
    
    /**
     * Wrapper around AbstractWorkload.stopTransaction() that writes the transaction
     * trace element out to file and then removes it from our cache data structures.
     */
    @Override
    public void stopTransaction(Object xact_handle) {
        super.stopTransaction(xact_handle);
        //
        // Write the trace object out to our file if it is not null
        //
        if (xact_handle != null && xact_handle instanceof TransactionTrace) {
            TransactionTrace xact = (TransactionTrace)xact_handle;
            Database catalog_db = this.xact_db_xref.remove(xact);
            
            if (catalog_db == null) {
                LOG.warn("The database catalog handle is null: " + xact);
            } else {
                if (this.out == null) {
                    LOG.warn("No output path is set. Unable to log trace information to file");
                } else {
                    writeTransactionToStream(catalog_db, xact, this.out);
                }
            }
            // Remove from cache
            this.removeTransaction(xact);
        }
        return;
    }
    
    public static void writeTransactionToStream(Database catalog_db, TransactionTrace xact, OutputStream output) {
        try {
            output.write(xact.toJSONString(catalog_db).getBytes());
            output.write("\n".getBytes());
            output.flush();
            LOG.debug("Wrote out new trace record for " + xact + " with " + xact.getQueries().size() + " queries");
        } catch (Exception ex) {
            LOG.fatal(ex.getMessage());
            ex.printStackTrace();
        }
    }
    
    @Override
    public String toString() {
        return (this.getClass().getSimpleName() + "{" + this.input_path.getName() + "}");
    }

    public static void main(String[] args) throws Exception {
        ArgumentsParser parsed_args = ArgumentsParser.load(args);
        
        /*
        if (stats != null && stats_path != null) {
            LOG.info("Writing workload statistics out to file '" + stats_path + "'");
            FileOutputStream stats_out = new FileOutputStream(stats_path);
            stats_out.write(ProcedureStatistics.toJSONString(stats).getBytes());    
            stats_out.close();
        }
        */
        
        
//        Map<Long, Integer> wid_counts = new HashMap<Long, Integer>(); 
        for (AbstractTraceElement<?> element : parsed_args.workload) {
            System.out.println(element);
//            if (element instanceof TransactionTrace) {
//                TransactionTrace xact = (TransactionTrace)element;
//                Long w_id = xact.getParam(0);
//                int cnt = 1;
//                if (wid_counts.containsKey(w_id)) cnt += wid_counts.get(w_id);
//                wid_counts.put(w_id, cnt);
//            }
        } // FOR
//        for (Long w_id : wid_counts.keySet()) {
//           System.out.println(w_id + ": " + wid_counts.get(w_id)); 
//        }
    }
}
