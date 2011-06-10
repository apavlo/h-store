package edu.brown.designer.placement;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.*;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Table;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.designer.MemoryEstimator;
import edu.brown.hashing.DefaultHasher;
import edu.brown.statistics.Histogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
import edu.brown.workload.TransactionTrace;

class TxnPartition {
    int basePartition;
    List<Set<Integer>> partitions = new ArrayList<Set<Integer>>();
    
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
public class TransformTransactionTraces {

    private static final Logger LOG = Logger.getLogger(TransformTransactionTraces.class);
    
    public static void transform(List<TransactionTrace> txn_traces, PartitionEstimator est, Database catalogDb, long partition_size) {
        Histogram hist = new Histogram();
        List<TxnPartition> al_txn_partitions = new ArrayList<TxnPartition>();
        
        /** keep the count on the number of times that each transaction access a partition per batch. **/
        Map<Integer, Integer> txn_partition_counts = new HashMap<Integer, Integer>();
        TxnPartition txn_partitions;

        int total_num_partitions = CatalogUtil.getNumberOfPartitions(catalogDb);
        int total_num_transactions = txn_traces.size(); // ???
        
        for (TransactionTrace trace : txn_traces) {
            int base_partition = 0;
            try {
                base_partition = est.getBasePartition(trace);
            } catch (Exception e1) {
                // TODO Auto-generated catch block
                e1.printStackTrace();
            }
            txn_partitions = new TxnPartition(base_partition);
            int batch = 0;
            for (Integer batch_id : trace.getBatchIds()) {
                // list of query traces
                Set<Integer> query_partitions = new HashSet<Integer>();
                for (QueryTrace qt :trace.getBatches().get(batch_id)) {
                    try {
                        query_partitions.addAll(est.getAllPartitions(qt, base_partition));
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
                // calculate the number of times partition X is called by batch Y - 
                // not including the base partition, should we?
                for (Integer partition_per_batch : query_partitions) {
                	if (txn_partition_counts.containsKey(partition_per_batch)) {
                		int count = txn_partition_counts.get(partition_per_batch);
                		count++;
                		txn_partition_counts.put(partition_per_batch, count);
                	} else {
                		txn_partition_counts.put(partition_per_batch, 1);                		
                	}
                }
                txn_partitions.getPartitions().add(query_partitions);
                batch++;
            }
            
            if (!hist.contains(txn_partitions.toString())) {
                hist.put(txn_partitions.toString(), 1);
            } else {
                long count = hist.get(txn_partitions.toString());
                count++;
                hist.put(txn_partitions.toString(), count);
            }
            al_txn_partitions.add(txn_partitions);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(CatalogUtil.getNumberOfHosts(catalogDb) + " " + CatalogUtil.getNumberOfSites(catalogDb) + " " + CatalogUtil.getNumberOfPartitions(catalogDb));
        // now compute the heat for each partition
        int current_heat = 0;
        for (int i = 0; i < total_num_partitions; i++) {
        	if (txn_partition_counts.containsKey(i)) {
        		sb.append("partition: " + i + " heat is: " + (txn_partition_counts.get(i) / total_num_transactions));
        	} else {
        		sb.append("partition: " + i + " heat is 0");        		
        	}
        }
        
        // write the file
        Writer writer;
        try {
            writer = new FileWriter("specifcy filepath");
            writer.append(sb.toString());
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
        
        LOG.info("Partition Histogram");
        LOG.info(hist.toString());
        writeFile(al_txn_partitions);
//        for (TxnPartition tp : al_txn_partitions) {
//            LOG.info(tp.toString());                    
//        }
    }
    
    /**
     * File format like the following:
     * # of batches # of transactions
     * base partition
     * [partitions batch 0 touches]
     * [partitions batch 1 touches]
     * ...
     * ...
     * [partitions batch n touches]
     * EOF
     * @param xact_partitions
     */
    public static void writeFile(List<TxnPartition> xact_partitions) {
        assert (xact_partitions.size() > 0);
        TxnPartition xact_partition = xact_partitions.get(0);
        StringBuilder sb = new StringBuilder();
        sb.append(xact_partition.getPartitions().size() + " " + xact_partitions.size() + "\n");
        for (TxnPartition txn_partition: xact_partitions) {
            sb.append(txn_partition.basePartition + "\n");
            for (Set<Integer> partition_set : txn_partition.getPartitions()) {
                for (Integer p_id : partition_set) {
                    sb.append(p_id + " ");
                }
                sb.append("\n");
            }
        }
        Writer writer;
        try {
            writer = new FileWriter("/home/sw47/Desktop/transaction.trace");
            writer.append(sb.toString());
            writer.flush();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        
        MemoryEstimator estimator = new MemoryEstimator(args.stats, new DefaultHasher(args.catalog_db, CatalogUtil.getNumberOfPartitions(args.catalog_db)));
        long partition_size = estimator.estimate(args.catalog_db, CatalogUtil.getNumberOfPartitions(args.catalog_db));
        Procedure catalog_proc = args.catalog_db.getProcedures().get("GetTableCounts"); // random procedure from tm1?
        PartitionEstimator p_estimator = new PartitionEstimator(args.catalog_db);
        transform(args.workload.getTraces(catalog_proc), p_estimator, args.catalog_db, partition_size);
    }

}