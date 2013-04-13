package edu.brown.workload;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections15.set.ListOrderedSet;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;

import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.RandomDistribution;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;
import edu.brown.utils.ProjectType;

/**
 * 
 * @author pavlo
 *
 */
public abstract class FixWorkload {
    private static final Logger LOG = Logger.getLogger(FixWorkload.class.getName());
        
    private static final int OL_SUPPLY_REMOTE = 50; // %
    private static final int OL_SUPPLY_REMOTE_ITEM = 50; // % 

    
    /**
     * 
     * @param catalogContext
     * @throws Exception
     */
    public static void addZipfianAffinity(ArgumentsParser args, double sigma) throws Exception {
        
        //
        // Figure out how many warehouses we have
        //
        ListOrderedSet<Integer> warehouses = new ListOrderedSet<Integer>();
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (!xact.getCatalogItemName().equals("neworder")) continue;
                warehouses.add(((Long)xact.getParam(0)).intValue());
            }
        } // FOR
        
        //
        // Create a synthetic affinity between different warehouses
        //
        Map<Integer, RandomDistribution.Zipf> distributions = new HashMap<Integer, RandomDistribution.Zipf>();
        Random rand = new Random();
        int num_warehouses = warehouses.size();
        for (Integer w_id : warehouses) {
            int other_id = w_id;
            while (other_id == w_id) other_id = warehouses.get(rand.nextInt(num_warehouses));
            distributions.put(w_id, new RandomDistribution.Zipf(rand, other_id, other_id + num_warehouses, sigma));
            //System.out.println(w_id + " => " + other_id);
        } // FOR
        FixWorkload.updateWorkloadWarehouses(args, distributions);
        return;
    }

    
    public static void addPairedAffinity(ArgumentsParser args) throws Exception {
        //
        // Put contiguous partition pairs together
        //
        int num_partitions = CatalogUtil.getNumberOfPartitions(args.catalog_db);
        Map<Integer, RandomDistribution.DiscreteRNG> distributions = new HashMap<Integer, RandomDistribution.DiscreteRNG>();
        
        Random rand = new Random();
        Integer last_partition = null;
        for (int i = 0; i < num_partitions; i++) {
            if (last_partition != null) {
                distributions.put(i, new RandomDistribution.Flat(rand, 0, num_partitions));
                distributions.put(last_partition, new RandomDistribution.Flat(rand, 0, num_partitions));
                
                //distributions.put(i, new RandomDistribution.Zipf(rand, last_partition, last_partition + num_partitions, 1.1));
                //distributions.put(last_partition, new RandomDistribution.Zipf(rand, i, i + num_partitions, 1.1));
                last_partition = null;
            } else {
                last_partition = i;
            }
        } // FOR
        FixWorkload.updateWorkload(args, distributions);
        return;
    }
    
    /**
     * 
     * @param args
     * @param distributions
     * @param histograms
     */
    private static void updateWorkload(ArgumentsParser args, Map<Integer, ? extends RandomDistribution.DiscreteRNG> distributions) {
        // Fix OL_SUPPLY_ID
        
        //
        // Lame: Generate a mapping of partition ids to warehouses, so that we can do a
        // a quick reverse lookup
        //
        Map<Integer, ObjectHistogram> histograms = new HashMap<Integer, ObjectHistogram>();
        SortedMap<Integer, ListOrderedSet<Integer>> partition_warehouse_xref = new TreeMap<Integer, ListOrderedSet<Integer>>();
        int num_partitions = args.hasher.getNumPartitions();
        System.out.println("Num of Partitions: " + num_partitions);
        for (int i = 0; i < num_partitions; i++) {
            partition_warehouse_xref.put(i, new ListOrderedSet<Integer>());
            histograms.put(i, new ObjectHistogram());
        }
        Set<Integer> warehouse_ids = new HashSet<Integer>();
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (!xact.getCatalogItemName().equals("neworder")) continue;
                int w_id = ((Long)xact.getParam(0)).intValue();
                int part_id = args.hasher.hash(w_id);    
                partition_warehouse_xref.get(part_id).add(w_id);
                warehouse_ids.add(w_id);
            }
        } // FOR
        StringBuilder buffer = new StringBuilder();
        buffer.append("Partition ID -> Warehouse ID\n");
        for (Integer partition_id : partition_warehouse_xref.keySet()) {
            buffer.append(partition_id).append(": ")
                  .append(partition_warehouse_xref.get(partition_id).toString()).append("\n");
        }
        LOG.info(buffer);
        
        int changed_cnt = 0;
        int total = 0;
        Random rand = new Random();
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (!xact.getCatalogItemName().equals("neworder")) continue;
                int param_idx = 5;
                total++; 
                Object ol_supply_ids[] = xact.getParam(param_idx);
                int orig_w_id = ((Long)xact.getParam(0)).intValue();
                int orig_part_id = args.hasher.hash(orig_w_id);
                
                RandomDistribution.DiscreteRNG rng = distributions.get(orig_part_id);
                if (rng == null) {
                    LOG.error("Original Partition Id: " + orig_part_id);
                    LOG.error(distributions);
                    System.exit(0);
                }
                
                boolean changed = false;
                for (int i = 0; i < ol_supply_ids.length; i++) {
                    if (rand.nextInt(100) <= OL_SUPPLY_REMOTE) {
                        Integer new_part_id = rng.nextInt();
                        if (new_part_id >= num_partitions) new_part_id = (new_part_id % num_partitions);
                        histograms.get(orig_part_id).put(new_part_id);
                        
                        // For the newly select partition id, randomly select a warehouse
                        // that will map back to this partition
                        ListOrderedSet<Integer> w_ids = partition_warehouse_xref.get(new_part_id);
                        assert(w_ids != null) : "Missing warehouse ids for partition " + new_part_id;
                        assert(!w_ids.isEmpty());
                        ol_supply_ids[i] = w_ids.get(rand.nextInt(w_ids.size())).longValue();
                        changed = true;
                        
                        int getStockInfo_cnt = 0;
                        int updateStock_cnt = 0;
                        int createOrderLine_cnt = 0;
                        for (QueryTrace query : xact.getQueries()) {
                            if (query.getCatalogItemName().startsWith("getStockInfo")) {
                                if (getStockInfo_cnt++ == i) query.setParam(1, ol_supply_ids[i]);
                            } else if (query.getCatalogItemName().equals("updateStock")) {
                                if (updateStock_cnt++ == i) query.setParam(5, ol_supply_ids[i]);
                            } else if (query.getCatalogItemName().equals("createOrderLine")) {
                                if (createOrderLine_cnt++ == i) query.setParam(5, ol_supply_ids[i]);
                            }
                        } // FOR
                        //System.out.println(xact.debug(catalog_db));
                        //System.exit(1);
                    //
                    // If we're not changing it, at least make sure it's the same value as the procedure param
                    //
                    } else {
                        ol_supply_ids[i] = orig_w_id;
                        histograms.get(orig_part_id).put(orig_part_id);
                    }
                } // FOR
                xact.setParam(param_idx, ol_supply_ids);
                if (changed) changed_cnt++;
            }
        } // FOR
        LOG.info("Updated " + changed_cnt + "/" + total + " transactions");
        
        buffer = new StringBuilder();
        buffer.append("Partition Histograms:\n");
        for ( Entry<Integer, ObjectHistogram> entry : histograms.entrySet()) {
            Integer partition_id = entry.getKey();
            buffer.append("Partition: " + partition_id + " [" + distributions.get(partition_id).getMin() + "]\n");
            buffer.append(entry.getValue()).append("\n");
        } // FOR
        LOG.info(buffer.toString());
        return;
    }
    
    
    /**
     * 
     * @param args
     * @param distributions
     * @param histograms
     */
    private static void updateWorkloadWarehouses(ArgumentsParser args, Map<Integer, ? extends RandomDistribution.DiscreteRNG> distributions) throws Exception {
        // Fix OL_SUPPLY_ID
        
        //
        // Lame: Generate a mapping of partition ids to warehouses, so that we can do a
        // a quick reverse lookup
        //
        Map<Integer, ObjectHistogram> histograms = new HashMap<Integer, ObjectHistogram>();
        Set<Integer> warehouse_ids = new HashSet<Integer>();
        
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (!xact.getCatalogItemName().equals("neworder")) continue;
                int w_id = ((Long)xact.getParam(0)).intValue();
                warehouse_ids.add(w_id);
                if (!histograms.containsKey(w_id)) {
                    histograms.put(w_id, new ObjectHistogram());
                }
            }
        } // FOR
        int num_warehouses = warehouse_ids.size();
        System.out.println("Num of Warehouses: " + num_warehouses);
        assert(num_warehouses <= 40);
        
        int changed_cnt = 0;
        int total = 0;
        Random rand = new Random();
        for (AbstractTraceElement<?> element : args.workload) {
            if (element instanceof TransactionTrace) {
                TransactionTrace xact = (TransactionTrace)element;
                if (!xact.getCatalogItemName().equals("neworder")) continue;
                int param_idx = 5;
                total++; 
                Object ol_supply_ids[] = xact.getParam(param_idx);
                int orig_w_id = ((Long)xact.getParam(0)).intValue();
                
                RandomDistribution.DiscreteRNG rng = distributions.get(orig_w_id);
                if (rng == null) {
                    LOG.error("Original Warehouse Id: " + orig_w_id);
                    LOG.error(distributions);
                    System.exit(0);
                }
                
                //
                // Let there be some probability that we are going to go to another warehouse
                // to grab our data. For now we'll have all the remote items be in the same
                // warehouse
                //
                Integer remote_w_id = null; 
                int updated_items = 0;
                if (rand.nextInt(100) <= OL_SUPPLY_REMOTE) {
                    remote_w_id = rng.nextInt();
                    if (remote_w_id >= num_warehouses) remote_w_id = (remote_w_id % num_warehouses) + 1;
                    histograms.get(orig_w_id).put(remote_w_id);
                } else {
                    histograms.get(orig_w_id).put(orig_w_id);
                }
                
                for (int i = 0; i < ol_supply_ids.length; i++) {
                    if (remote_w_id != null && rand.nextInt(100) <= OL_SUPPLY_REMOTE_ITEM) {
                        ol_supply_ids[i] = remote_w_id;
                        updated_items++;
                        
                        int getStockInfo_cnt = 0;
                        int updateStock_cnt = 0;
                        int createOrderLine_cnt = 0;
                        for (QueryTrace query : xact.getQueries()) {
                            if (query.getCatalogItemName().startsWith("getStockInfo")) {
                                if (getStockInfo_cnt++ == i) query.setParam(1, ol_supply_ids[i]);
                            } else if (query.getCatalogItemName().equals("updateStock")) {
                                if (updateStock_cnt++ == i) query.setParam(5, ol_supply_ids[i]);
                            } else if (query.getCatalogItemName().equals("createOrderLine")) {
                                if (createOrderLine_cnt++ == i) query.setParam(5, ol_supply_ids[i]);
                            }
                        } // FOR
                        //System.out.println(xact.debug(catalog_db));
                        //System.exit(1);
                    //
                    // If we're not changing it, at least make sure it's the same value as the procedure param
                    //
                    } else {
                        ol_supply_ids[i] = orig_w_id;
                    }
                } // FOR
                xact.setParam(param_idx, ol_supply_ids);
                if (updated_items > 0) changed_cnt++;
            }
        } // FOR
        LOG.info("Updated " + changed_cnt + "/" + total + " transactions");
        
        StringBuilder buffer = new StringBuilder();
        buffer.append("Warehouse Histograms:\n");
        for (Integer w_id : histograms.keySet()) {
            ObjectHistogram hist = histograms.get(w_id);
            buffer.append("Partition: " + w_id + " [" + distributions.get(w_id).getMin() + "]\n");
            buffer.append(hist).append("\n");
            hist.save(new File("histograms/" + w_id + ".hist"));
        } // FOR
        LOG.info(buffer.toString());
        
        return;
    }
//    
//    private static final void fixTM1(Workload workload) throws Exception {
//        // Fix UpdateSubscriberData
//        long fix_ctr = 0;
//        for (TransactionTrace txn_trace : workload.getTransactions()) {
//            if (!txn_trace.getCatalogItemName().equals("UpdateSubscriberData")) continue;
//            
//            Object params[] = txn_trace.getParams();
//            Object temp = params[0];
//            txn_trace.setParam(0, params[1]);
//            txn_trace.setParam(1, temp);
//            fix_ctr++;
//        }
//        LOG.info("Fixed " + fix_ctr + " UpdateSubscriberData txns!");
//        return;
//    }
    
    private static final void updateTraceIds(Database catalog_db, Workload workload, String output_path) throws Exception {
        FileOutputStream output = new FileOutputStream(output_path);
        
        for (TransactionTrace txn_trace : workload.getTransactions()) {
            Workload.writeTransactionToStream(catalog_db, txn_trace, output);
        } // FOR
    }
    
    /**
     * @param args
     */
    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        args.require(
            ArgumentsParser.PARAM_CATALOG,
            ArgumentsParser.PARAM_WORKLOAD,
            ArgumentsParser.PARAM_WORKLOAD_OUTPUT
        );
        String output_path = args.getParam(ArgumentsParser.PARAM_WORKLOAD_OUTPUT);
        assert(output_path != null);
        ProjectType type = args.catalog_type;
        
        updateTraceIds(args.catalog_db, args.workload, output_path);
        
        
//        // Fix the workload!
//        if (type.equals(ProjectType.TPCC)) {
//            if (args.getOptParamCount() == 3) {
//                String id_suffix = args.getOptParam(0);
//                int min_id = args.getIntOptParam(1);
//                int max_id = args.getIntOptParam(2);
//                LOG.info("Expanding partitioning parameters using '" + id_suffix + "'");
//                FixWorkload.expandPartitionParameters(args, id_suffix, min_id, max_id);
//            } else {
//                double sigma = Double.valueOf(args.getOptParam(0));
//                LOG.info("Adding zipfian partition affinity to workload with sigma=" + sigma);
//                FixWorkload.addZipfianAffinity(args, sigma);
//                //FixWorkload.addPairedAffinity(args);    
//            }
//        } else if (type.equals(ProjectType.TM1)) {
//            // Expand TM1
//            // int start_id = Collections.max(args.workload.element_ids) + 1;
//            // FixWorkload.duplicateWorkload(args, start_id);
//            fixTM1(args.workload);
//            
//        }
        
        //
        // We need to write this things somewhere now...
        //
        //System.out.println(args.workload.debug(args.catalog_db));
        //args.workload.save(output_path, args.catalog_db);
        LOG.info("Wrote updated workload to '" + output_path + "'");

        return;
    }

}
