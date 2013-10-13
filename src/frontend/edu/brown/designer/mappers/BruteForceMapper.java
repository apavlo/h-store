/**
 * 
 */
package edu.brown.designer.mappers;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Table;

import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.ArgumentsParser;

/**
 * @author pavlo
 */
public class BruteForceMapper extends AbstractMapper {

    private final Map<Integer, ObjectHistogram> histograms = new HashMap<Integer, ObjectHistogram>();
    private Solution best_solution = null;
    private Solution worst_solution = null;
    private final Map<Thread, AtomicInteger> thread_checkpoints = new HashMap<Thread, AtomicInteger>();
    private final Map<Thread, int[]> thread_partition_order = new HashMap<Thread, int[]>();

    /**
     * Constructor
     * 
     * @param designer
     * @param info
     */
    public BruteForceMapper(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    /**
     * @param nodes
     * @param cores_per_node
     * @param partitions
     * @return
     */
    protected Set<Solution> generateAllSolutions(int nodes, int cores_per_node, List<Integer> partitions) {
        Set<Solution> solutions = new HashSet<Solution>();

        return (solutions);
    }

    /*
     * (non-Javadoc)
     * @see
     * edu.brown.designer.mappers.AbstractMapper#generate(edu.brown.designer
     * .DesignerHints, edu.brown.designer.partitioners.PartitionPlan)
     */
    @Override
    public PartitionMapping generate(DesignerHints hints, PartitionPlan pplan) throws Exception {
        PartitionMapping pmap = new PartitionMapping();

        //
        // Sites
        //
        Cluster catalog_cluster = (Cluster) info.catalogContext.database.getParent();
        int site_id = 0;
        for (Host catalog_host : catalog_cluster.getHosts()) {
            int num_sites = catalog_host.getCorespercpu() * catalog_host.getThreadspercore();
            for (int ctr = 0; ctr < num_sites; ctr++) {
                SiteEntry site = new SiteEntry(site_id);
                pmap.assign(catalog_host, site);
                site_id++;
            } // FOR
        } // FOR

        //
        // Table Fragments
        //
        for (Table root : pplan.getNonReplicatedRoots()) {
            for (int ctr = 0; ctr < site_id; ctr++) {
                SiteEntry site = pmap.getSite(ctr);
                FragmentEntry fragment = new FragmentEntry(root, ctr);
                pmap.assign(site, fragment);
            } // FOR
        } // FOR
        pmap.initialize();
        return (pmap);
    }

    /**
     * Hack for HPTS
     * 
     * @param num_warehouses
     */
    public void search(final int num_warehouses) throws Exception {
        //
        // Load in histograms
        //
        final int start_id = 1;
        List<Integer> partitions = new ArrayList<Integer>();
        for (int i = start_id; i <= num_warehouses; i++) {
            File path = new File("histograms/" + i + ".hist");
            ObjectHistogram h = new ObjectHistogram();
            h.load(path, null);
            this.histograms.put(i, h);
            partitions.add(i);
        } // FOR

        int num_nodes = 5;
        int cores_per_node = 4;
        int num_threads = 10;
        final Solution start = null; // new Solution(num_nodes, cores_per_node);

        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < num_threads; i++) {
            //
            // Create a random list of partitions
            //
            final int partition_order[] = new int[1 + (num_warehouses - start_id)];
            if (num_threads > 1)
                Collections.shuffle(partitions);
            for (int j = 0; j < partition_order.length; j++) {
                partition_order[j] = partitions.get(j);
            }
            LOG.info("Starting new traversal thread: " + partitions);

            Thread thread = new Thread() {
                public void run() {
                    Solution clone = null; // start.clone();
                    BruteForceMapper.this.thread_checkpoints.put(this, new AtomicInteger(0));
                    BruteForceMapper.this.thread_partition_order.put(this, partition_order);
                    BruteForceMapper.this.traverse(clone, 0, partition_order.length);
                }
            };
            thread.start();
            threads.add(thread);
        } // FOR

        for (Thread thread : threads) {
            thread.join();
        } // FOR

        System.out.println("--------------------------------\nBest Solution:");
        System.out.println(this.best_solution);
        System.out.println("--------------------------------\nWorst Solution:");
        System.out.println(this.worst_solution);

        this.best_solution.toHasher().save(new File("histograms/hasher.profile"));
    }

    private List<Solution> createSolutions(int num_nodes, int cores_per_node, List<Integer> partitions) {
        return (null);
    }

    private void traverse(Solution solution, int level, int max_id) {
        /*
         * // // For through the current solution and add our partition id to
         * any node that // has a free slot and then continue down the line //
         * boolean complete = (level + 1 == max_id); int part_id =
         * this.thread_partition_order.get(Thread.currentThread())[level];
         * assert(part_id >= 0); for (Integer node_id : solution.keySet()) {
         * Node node = solution.get(node_id); if (node.getFreeSlots() > 0) {
         * Solution clone = solution.clone(); // System.out.println(clone);
         * clone.addPartition(node_id, part_id);
         * clone.setCost(this.cost(clone)); if (this.best_solution == null ||
         * clone.getCost() < this.best_solution.getCost()) { // // Complete
         * Solution // if (complete) { synchronized (this) { this.best_solution
         * = clone; } System.out.println(this.best_solution); // System.exit(1);
         * } else if (level + 1 < max_id) { this.traverse(clone, level + 1,
         * max_id); } } else if (this.worst_solution == null || clone.getCost()
         * > this.worst_solution.getCost()) { if (this.best_solution != null)
         * assert(!this.best_solution.equals(clone)); this.worst_solution =
         * clone; } } } // FOR int count =
         * this.thread_checkpoints.get(Thread.currentThread
         * ()).incrementAndGet(); if (count % 1000000 == 0) {
         * LOG.info(solution.getPartitions()); LOG.debug(solution);
         * this.thread_checkpoints.get(Thread.currentThread()).set(0); }
         */
    }

    private Double cost(Solution solution) {
        double total_cost = 0.0d;
        /*
         * for (Integer node_id : solution.keySet()) { Node node =
         * solution.get(node_id); double node_cost = 0.0d; for (Integer part_id
         * : node) { Histogram hist = this.histograms.get(part_id); for (Object
         * value : hist.values()) { int count = hist.get(value); Integer
         * other_id = null; if (value instanceof Long) { other_id =
         * ((Long)value).intValue(); } else { other_id = (Integer)value; } if
         * (!solution.hasPartition(other_id)) continue; // Same Partition if
         * (part_id.equals(other_id)) { node_cost += 0.0d; // Same Node } else
         * if (node.contains(other_id)) { node_cost += (0.5d * count); // Cross
         * Node } else { node_cost += (5.0d * count); } } // FOR } // FOR
         * total_cost += node_cost; } // FOR
         */return (total_cost);
    }

    public static void main(String[] vargs) throws Exception {
        ArgumentsParser args = ArgumentsParser.load(vargs);
        new BruteForceMapper(null, null).search(20);
    }
}
