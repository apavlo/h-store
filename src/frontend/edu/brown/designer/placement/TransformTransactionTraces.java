package edu.brown.designer.placement;

import java.io.File;
import java.util.ArrayList;
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
import edu.brown.utils.StringUtil;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;


public class TransformTransactionTraces {

    private static final Logger LOG = Logger.getLogger(TransformTransactionTraces.class);
    
    private static final double DEFAULT_DIFFERENT_PARTITION_PENALTY = 1d;
    private static final double DEFAULT_DIFFERENT_SITE_PENALTY      = 100d;
    private static final double DEFAULT_DIFFERENT_HOST_PENALTY      = 10000d;
    
    protected static class TxnPartition {
        final int basePartition;
        final List<Set<Integer>> partitions = new ArrayList<Set<Integer>>();
        
        public TxnPartition(int basePartition) {
            this.basePartition = basePartition;
        }
        
        public List<Set<Integer>> getPartitions() {
            return partitions;
        }
        
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("base partition: " + String.valueOf(basePartition) + " ");
            for (Set<Integer> batch : partitions) {
                for (Integer partition_id : batch) {
                    sb.append(String.valueOf(partition_id) + " ");
                }
            }
            return sb.toString();
        }
    }
    
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
    	
        final int total_num_hosts = CatalogUtil.getNumberOfHosts(catalogDb);
        final int total_num_partitions = CatalogUtil.getNumberOfPartitions(catalogDb);
        final int sites_per_host = CatalogUtil.getNumberOfSites(catalogDb) / CatalogUtil.getNumberOfHosts(catalogDb);
        
        Histogram<Integer> hist = new Histogram<Integer>();
        List<TxnPartition> al_txn_partitions = new ArrayList<TxnPartition>();

        /** keep the count on the number of times that each transaction access a partition per batch. **/
        TxnPartition txn_partitions;

        int total_num_transactions = txn_traces.size(); // ???
        int[][] output_matrix = new int[total_num_partitions][total_num_partitions];
        int[] num_partition_batches = new int[total_num_partitions];
        
        for (TransactionTrace trace : txn_traces) {
            int base_partition = 0;
            try {
                base_partition = est.getBasePartition(trace);
                hist.put(base_partition);
            } catch (Exception e1) {
                throw new RuntimeException(e1);
            }
            txn_partitions = new TxnPartition(base_partition);
            
            for (Integer batch_id : trace.getBatchIds()) {
                Set<Integer> partitions_touched_per_batch = new HashSet<Integer>();
                List<QueryTrace> batch_queries = trace.getBatches().get(batch_id);
                
                // list of query traces
                Set<Integer> total_query_partitions = new HashSet<Integer>();
                if (isMultiPartitionBatch(batch_queries, est, base_partition)) {
                    for (QueryTrace qt : batch_queries) {
                        try {
                        	Set<Integer> batch_partitions = est.getAllPartitions(qt, base_partition);
                        	for (int partition : batch_partitions) {
                    			if (!partitions_touched_per_batch.contains(partition)) {
                    				partitions_touched_per_batch.add(partition);
                    			}
                        	}                    		
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                	// increment # of multi-partition batches from the base partition
                    num_partition_batches[base_partition]++;
                	// calculate which other partitions were touched by the multi partition transaction
                    output_matrix[base_partition][base_partition]++;
                    for (int partition_num : partitions_touched_per_batch) {
                    	//output_matrix[base_partition][partition_num]++;                    		
                    	if (partition_num != base_partition) {
                        	output_matrix[base_partition][partition_num]++;                    		
                    	}
                    }
                }
                txn_partitions.getPartitions().add(total_query_partitions);
            }
            al_txn_partitions.add(txn_partitions);
        }
        

        StringBuilder sb = new StringBuilder();
        sb.append(String.format("%d %d %d %d\n", total_num_hosts, sites_per_host, total_num_partitions, memory_per_host));
        sb.append(StringUtil.join(" ", penalties) + "\n");
        
        // HEAT
        for (int hist_value : hist.values()) {
        	//LOG.info(hist_value  + ": " + hist.get(hist_value));
    		sb.append(((hist.get(hist_value) * 1.0) / total_num_transactions) + " " + partition_size + "\n");
        }
        
        // AFFINITY
        for (int i = 0; i < output_matrix.length; i++) {
            for (int j = 0; j < output_matrix.length; j++) {
                double aff = output_matrix[i][j] * 1.0 / num_partition_batches[i];
                LOG.debug(String.format("%d -> %d: %.3f", i, j, aff));
                sb.append(aff).append(" ");
            } // FOR
            sb.append("\n");
        } // FOR

        return (sb.toString());
    }
    
    // takes in a list of query traces for a given batch 
    // returns: true if the batch is multi-partitioned, false otherwise
    public static boolean isMultiPartitionBatch(List<QueryTrace> traces, PartitionEstimator est, int base_partition) {
        for (QueryTrace qt : traces) {
        	Set<Integer> batch_partitions;
			try {
				batch_partitions = est.getAllPartitions(qt, base_partition);
	        	for (int partition : batch_partitions) {
	        		if (partition != base_partition) {
	        			return true;
	        		}
	        	}                    		        	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
        return false;
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
                ArgumentsParser.PARAM_CATALOG,
                ArgumentsParser.PARAM_WORKLOAD,
                ArgumentsParser.PARAM_STATS,
                ArgumentsParser.PARAM_SIMULATOR_HOST_MEMORY
            );

        long memory_per_host = args.getLongParam("simulator.host.memory");
        String output_path = args.getOptParam(0);
        double penalties[] = {
            DEFAULT_DIFFERENT_PARTITION_PENALTY,
            DEFAULT_DIFFERENT_SITE_PENALTY,
            DEFAULT_DIFFERENT_HOST_PENALTY
        };
        
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        MemoryEstimator estimator = new MemoryEstimator(args.stats, p_estimator.getHasher());
        long partition_size = (estimator.estimateTotalSize(args.catalog_db) / CatalogUtil.getNumberOfPartitions(args.catalog));
        
        String contents = transform(args.workload.getTransactions(), p_estimator, args.catalog_db, memory_per_host, partition_size, penalties);
        
        // Write out to file
        File f = FileUtil.writeStringToFile(output_path, contents);
        LOG.info("Wrote Data Placement Comet input file to " + f.getAbsolutePath());
    }

}
