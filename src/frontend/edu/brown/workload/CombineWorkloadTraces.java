package edu.brown.workload;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;

/**
 * @author pavlo
 */
public class CombineWorkloadTraces {
    private static final Logger LOG = Logger.getLogger(CombineWorkloadTraces.class);
    
    /**
     * Combine a bunch of workloads into a single output stream (sorted by txn timestamps)
     * @param output
     * @param catalog_db
     * @param workloads
     */
    @SuppressWarnings("unchecked")
    public static void combineWorkloads(OutputStream output, Database catalog_db, Workload workloads[]) {
        Integer next_idxs[] = new Integer[workloads.length];
        Integer max_idxs[] = new Integer[workloads.length];
        List<TransactionTrace> txns[] = new List[workloads.length];
        long relative_starts[] = new long[workloads.length];
        int finished = 0;
        for (int i = 0; i < workloads.length; i++) {
            txns[i] = new ArrayList<TransactionTrace>(workloads[i].getTransactions());
            max_idxs[i] = txns[i].size();
            if (max_idxs[i] > 0) {
                relative_starts[i] = txns[i].get(0).getStartTimestamp();
                next_idxs[i] = 0;
            } else {
                next_idxs[i] = null;
                finished++;
            }
            LOG.info(String.format("Workload #%02d: %d txns", i, txns[i].size()));
        }
        ObjectHistogram<String> proc_histogram = new ObjectHistogram<String>(); 
        
        // This is basically a crappy merge sort...
        long ctr = 0;
        long new_txn_id = 10000;
        while (true) {
            long min_timestamp = Long.MAX_VALUE;
            Integer min_idx = null;
            for (int i = 0; i < workloads.length; i++) {
                if (next_idxs[i] == null) continue;
                TransactionTrace xact = txns[i].get(next_idxs[i]); 
                // System.err.println("[" + i + "] " + xact + " - " + xact.getStartTimestamp());
                long start = xact.getStartTimestamp() - relative_starts[i]; 
                if (start < min_timestamp) {
                    min_timestamp = start; 
                    min_idx = i;
                }
            } // FOR
            if (min_idx == null) break;
            
            // Insert the txn into the output
            int current_offset = next_idxs[min_idx];
            TransactionTrace xact = txns[min_idx].get(current_offset);
            
            // Update txn ids so that we don't get duplicates
            // Fix the timestamps so that they are all the same
            xact.setTransactionId(new_txn_id++);
            xact.start_timestamp -= relative_starts[min_idx];
            xact.stop_timestamp -= relative_starts[min_idx];
//            if (next_idxs[min_idx] == 0) System.err.println(xact.debug(catalog_db));
            for (QueryTrace query_trace : xact.getQueries()) {
                query_trace.start_timestamp -= relative_starts[min_idx];
                if (query_trace.stop_timestamp == null) query_trace.stop_timestamp = query_trace.start_timestamp;
                query_trace.stop_timestamp -= relative_starts[min_idx];
            } // FOR
            Workload.writeTransactionToStream(catalog_db, xact, output, false);
            proc_histogram.put(xact.getCatalogItemName());
            
            // And increment the counter for the next txn we could use from this workload
            // If we are out of txns, set next_txns to null
            if (++next_idxs[min_idx] >= max_idxs[min_idx]) {
                next_idxs[min_idx] = null;
                LOG.info(String.format("Finished Workload #%02d [%02d/%02d]", min_idx, ++finished, next_idxs.length));
            }
            ctr++;
        } // WHILE
        LOG.info("Successful merged all " + ctr + " txns");
        LOG.info("Procedures Histogram:\n" + proc_histogram);
        return;
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(ArgumentsParser.PARAM_CATALOG, ArgumentsParser.PARAM_WORKLOAD_OUTPUT);
        
        List<File> workload_files = new ArrayList<File>();
        for (int i = 0, cnt = args.getOptParamCount(); i < cnt; i++) {
            File base_workload_path = new File(args.getOptParam(i));
            File base_directory = base_workload_path.getParentFile();
            String base_workload_name = base_workload_path.getName();
            if (base_workload_name.endsWith("*")) {
                base_workload_name = base_workload_name.substring(0, base_workload_name.length()-2);
            }
            
            workload_files.addAll(FileUtil.getFilesInDirectory(base_directory, base_workload_name));
            if (workload_files.isEmpty()) {
                LOG.fatal("No workload files starting with '" + base_workload_name + "' were found in '" + base_directory + "'");
                System.exit(1);
            }
        }
        Collections.sort(workload_files);
        
        File output_path = new File(args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT));
        FileUtil.makeDirIfNotExists(output_path.getParent());
        
        int num_workloads = workload_files.size();
        assert(num_workloads > 0) : "No workloads specified";
        Workload workloads[] = new Workload[num_workloads];
        LOG.info("Combining " + num_workloads + " workloads into '" + output_path + "'");
        for (int i = 0; i < num_workloads; i++) {
            File input_path = workload_files.get(i);
            LOG.debug("Loading workload '" + input_path + "'");
            workloads[i] = new Workload(args.catalog);
            workloads[i].load(input_path, args.catalog_db);
        } // FOR

        FileOutputStream output = new FileOutputStream(output_path);
        combineWorkloads(output, args.catalog_db, workloads);
        output.close();
    }

}
