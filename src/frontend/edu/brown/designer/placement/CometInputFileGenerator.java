package edu.brown.designer.placement;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.MemoryEstimator;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.FileUtil;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;


public class CometInputFileGenerator {

    private static final Logger LOG = Logger.getLogger(CometInputFileGenerator.class);
    
    private static final double DEFAULT_DIFFERENT_PARTITION_PENALTY = 1d;
    private static final double DEFAULT_DIFFERENT_SITE_PENALTY      = 100d;
    private static final double DEFAULT_DIFFERENT_HOST_PENALTY      = 10000d;
    
    /**
     * File format like the following:
     * # of hosts, # of sites, # of partitions, total memory per node (site), # sites per host
     * "penalty different partitions" "penalty different sites" "penalty different hosts"
     * heat (partition 1) total partition size (partition 1) total available memory (partition 1)
     * ...
     * ...
     * heat (partition n) total partition size (partition n) total available memory (partition n)
     * [affinity stuff]
     * partition 0 ..... partition n
     * ........... ..... ...........
     * ........... ..... ...........
     * partition n ..... partition n
     * EOF
     */
    public static String transform(List<TransactionTrace> txn_traces, PartitionEstimator est, Database catalogDb, long memory_per_host, long partition_size, double penalties[]) {
        final boolean d = LOG.isDebugEnabled();
    	
        final int total_num_hosts = CatalogUtil.getNumberOfHosts(catalogDb);
        final int total_num_partitions = CatalogUtil.getNumberOfPartitions(catalogDb);
        final int sites_per_host = CatalogUtil.getNumberOfSites(catalogDb) / CatalogUtil.getNumberOfHosts(catalogDb);
        
        int total_num_transactions = txn_traces.size(); // ???
        int[][] affinity = new int[total_num_partitions][total_num_partitions];
        Histogram<Integer> multipartition_batches = new Histogram<Integer>();
        
        Set<Integer> all_partitions = new HashSet<Integer>();
        Histogram<Integer> heat_histogram = new Histogram<Integer>();
        
        for (TransactionTrace trace : txn_traces) {
            int base_partition = 0;
            try {
                base_partition = est.getBasePartition(trace);
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
            
            all_partitions.clear();
            all_partitions.add(base_partition);
            
            for (Integer batch_id : trace.getBatchIds()) {
                Set<Integer> partitions_touched_per_batch = new HashSet<Integer>();
                
                Set<Integer> batch_partitions = null;
                boolean was_multipartitioned = false;
                for (QueryTrace qt : trace.getBatches().get(batch_id)) {
                    try {
                        batch_partitions = est.getAllPartitions(qt, base_partition);
                        if (batch_partitions.size() > 1 || batch_partitions.contains(base_partition) == false) {
                            partitions_touched_per_batch.addAll(batch_partitions);
                            was_multipartitioned = true;
                    	}
                        
                        // Always keep track of which partitions this txn touched
                        all_partitions.addAll(batch_partitions);
                        
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
                
                // increment # of multi-partition batches from the base partition
                if (was_multipartitioned) multipartition_batches.put(base_partition);
                
                // Add one to each partition that this txn touched
                heat_histogram.putAll(all_partitions);
                	
            	// calculate which other partitions were touched by the multi partition transaction
                for (int p : partitions_touched_per_batch) {
                	affinity[base_partition][p]++;                    		
                } // FOR
            }
        } // FOR
        

        StringBuilder sb = new StringBuilder();
        
        // CLUSTER INFO
        sb.append(String.format("%d %d %d %d\n", total_num_hosts, sites_per_host, total_num_partitions, memory_per_host));
        
        // PENALTIES
        for (int i = 0; i < penalties.length; i++) {
            if (i > 0) sb.append(" ");
            sb.append(penalties[i]);
        } // FOR
        sb.append("\n");
        
        // HEAT
        for (int partition : heat_histogram.values()) {
            double partition_heat = heat_histogram.get(partition, 0) / (double)total_num_transactions;
            // FIXME long partition_size = ??;
    		sb.append(String.format("%.6f %d\n", partition_heat, partition_size));
    		if (d) LOG.debug(String.format("Partition %02d: %d / %d = %f", partition, heat_histogram.get(partition, 0), total_num_transactions, partition_heat));
        } // FOR
        
        // AFFINITY
        for (int i = 0; i < affinity.length; i++) {
            long num_multip_txns = multipartition_batches.get(i, 0);
            for (int j = 0; j < affinity.length; j++) {
                double aff = (i == j ? 0d : (affinity[i][j] / (double)num_multip_txns));
                if (d) LOG.debug(String.format("%d -> %d: 1.0 - (%d / %d) = %.3f", i, j, affinity[i][j], num_multip_txns, aff));
                assert(aff >= 0.0 && aff <= 1.0) : String.format("Invalid Affinity %d -> %d: %.3f", i, j, aff);
                if (j > 0) sb.append(" ");
                sb.append(String.format("%.6f", aff));
            } // FOR
            sb.append("\n");
        } // FOR

        return (sb.toString());
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
                ArgumentsParser.PARAM_CATALOG,
                ArgumentsParser.PARAM_WORKLOAD,
                ArgumentsParser.PARAM_STATS,
                ArgumentsParser.PARAM_SIMULATOR_HOST_MEMORY
            );

        long memory_per_host = args.getLongParam(ArgumentsParser.PARAM_SIMULATOR_HOST_MEMORY);
        String output_path = args.getOptParam(0);
        double penalties[] = {
            DEFAULT_DIFFERENT_PARTITION_PENALTY,
            DEFAULT_DIFFERENT_SITE_PENALTY,
            DEFAULT_DIFFERENT_HOST_PENALTY
        };
        
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        MemoryEstimator estimator = new MemoryEstimator(args.stats, p_estimator.getHasher());
        long partition_size = (estimator.estimateTotalSize(args.catalog_db) / CatalogUtil.getNumberOfPartitions(args.catalog));
        partition_size /= 1048576; // 1 MB
        
        String contents = transform(args.workload.getTransactions(), p_estimator, args.catalog_db, memory_per_host, partition_size, penalties);
        
        // Write out to file
        File f = FileUtil.writeStringToFile(output_path, contents);
        LOG.info("Wrote Data Placement Comet input file to " + f.getAbsolutePath());
    }

}
