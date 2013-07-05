package edu.brown.hstore.estimators.markov;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Procedure;

import edu.brown.hstore.HStoreConstants;
import edu.brown.markov.MarkovGraph;
import edu.brown.markov.MarkovUtil;
import edu.brown.markov.MarkovVertex;
import edu.brown.markov.containers.MarkovGraphsContainer;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.utils.PartitionSet;
import edu.brown.workload.TransactionTrace;

public abstract class MarkovPathEstimatorDumper {
    private static final Logger LOG = Logger.getLogger(MarkovPathEstimatorDumper.class);

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_MAPPINGS,
            ArgumentsParser.PARAM_MARKOV
        );
        
        // Word up
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalogContext);
        
        // Create MarkovGraphsContainer
        File input_path = args.getFileParam(ArgumentsParser.PARAM_MARKOV);
        Map<Integer, MarkovGraphsContainer> m = MarkovUtil.load(args.catalogContext, input_path);
        
        // Blah blah blah...
        Map<Integer, MarkovEstimator> t_estimators = new HashMap<Integer, MarkovEstimator>();
        for (Integer id : m.keySet()) {
            t_estimators.put(id, new MarkovEstimator(args.catalogContext, p_estimator, m.get(id)));
        } // FOR
        
        final Set<String> skip = new HashSet<String>();
        
        Map<Procedure, AtomicInteger> totals = new TreeMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> correct_partitions_txns = new HashMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> correct_path_txns = new HashMap<Procedure, AtomicInteger>();
        Map<Procedure, AtomicInteger> multip_txns = new HashMap<Procedure, AtomicInteger>();
        for (Procedure catalog_proc : args.catalog_db.getProcedures()) {
            if (!catalog_proc.getSystemproc()) {
                totals.put(catalog_proc, new AtomicInteger(0));
                correct_partitions_txns.put(catalog_proc, new AtomicInteger(0));
                correct_path_txns.put(catalog_proc, new AtomicInteger(0));
                multip_txns.put(catalog_proc, new AtomicInteger(0));
            }
        } // FOR
        
        // Loop through each of the procedures and measure how accurate we are in our predictions
        for (TransactionTrace xact : args.workload.getTransactions()) {
            LOG.info(xact.debug(args.catalog_db));
            
            Procedure catalog_proc = xact.getCatalogItem(args.catalog_db);
            if (skip.contains(catalog_proc.getName())) continue;
            
            int partition = HStoreConstants.NULL_PARTITION_ID;
            try {
                partition = p_estimator.getBasePartition(catalog_proc, xact.getParams(), true);
            } catch (Exception ex) {
                ex.printStackTrace();
                assert(false);
            }
            assert(partition >= 0);
            totals.get(catalog_proc).incrementAndGet();
            
            MarkovGraph markov = m.get(partition).getFromParams(xact.getTransactionId(), partition, xact.getParams(), catalog_proc);
            if (markov == null) {
                LOG.warn(String.format("No MarkovGraph for %s at partition %d", catalog_proc.getName(), partition));
                continue;
            }
            
            // Check whether we predict the same path
            List<MarkovVertex> actual_path = markov.processTransaction(xact, p_estimator);
            MarkovEstimate est = MarkovPathEstimator.predictPath(markov, t_estimators.get(partition), xact.getParams());
            assert(est != null);
            List<MarkovVertex> predicted_path = est.getMarkovPath();
            if (actual_path.equals(predicted_path)) correct_path_txns.get(catalog_proc).incrementAndGet();
            
            LOG.info("MarkovEstimate:\n" + est);
            
            // Check whether we predict the same partitions
            PartitionSet actual_partitions = MarkovUtil.getTouchedPartitions(actual_path); 
            PartitionSet predicted_partitions = MarkovUtil.getTouchedPartitions(predicted_path);
            if (actual_partitions.equals(predicted_partitions)) correct_partitions_txns.get(catalog_proc).incrementAndGet();
            if (actual_partitions.size() > 1) multip_txns.get(catalog_proc).incrementAndGet();
            
                
//                System.err.println(xact.debug(args.catalog_db));
//                System.err.println(StringUtil.repeat("=", 120));
//                System.err.println(GraphUtil.comparePaths(markov, actual_path, predicted_path));
//                
//                String dotfile = "/home/pavlo/" + catalog_proc.getName() + ".dot";
//                GraphvizExport<Vertex, Edge> graphviz = MarkovUtil.exportGraphviz(markov, actual_path); 
//                FileUtil.writeStringToFile(dotfile, graphviz.export(catalog_proc.getName()));
////                skip.add(catalog_proc.getName());
//                System.err.println("\n\n");
        } // FOR
        
//        if (args.hasParam(ArgumentsParser.PARAM_MARKOV_OUTPUT)) {
//            markovs.save(args.getParam(ArgumentsParser.PARAM_MARKOV_OUTPUT));
//        }
        
        System.err.println("Procedure\t\tTotal\tSingleP\tPartitions\tPaths");
        for (Entry<Procedure, AtomicInteger> entry : totals.entrySet()) {
            Procedure catalog_proc = entry.getKey();
            int total = entry.getValue().get();
            if (total == 0) continue;
            
            int valid_partitions = correct_partitions_txns.get(catalog_proc).get();
            double valid_partitions_p = (valid_partitions / (double)total) * 100;
            
            int valid_paths = correct_path_txns.get(catalog_proc).get();
            double valid_paths_p = (valid_paths / (double)total) * 100;            
            
            int singlep = total - multip_txns.get(catalog_proc).get();
            double singlep_p = (singlep / (double)total) * 100;
            
            System.err.println(String.format("%-25s %d\t%.02f\t%.02f\t%.02f", catalog_proc.getName(),
                    total,
                    singlep_p,
                    valid_partitions_p,
                    valid_paths_p
            ));
        } // FOR
        
    }
    
}
