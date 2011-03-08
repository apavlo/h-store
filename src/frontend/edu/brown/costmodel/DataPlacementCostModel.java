package edu.brown.costmodel;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;

import sun.tools.attach.HotSpotVirtualMachine;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.TransactionTrace;
import edu.brown.workload.Workload;
import edu.brown.workload.Workload.Filter;

/**
 * Need to build an internal data structure that can quickly figure out whether certain partitions are 
 * in the same site versus the same node versus on different nodes. By the time it hits this cost model,
 * the database's partitioning parameters have already been assigned. The PartitionEstimator will be used 
 * to get back the set of partitions that each TransactionTrace will need touch. It will then estimate based 
 * on the relative location of those partitions with each other.
 * 
 * @author sw47
 *
 */
public class DataPlacementCostModel extends AbstractCostModel {

    /**
     * Default Constructor
     * 
     * @param catalog_db
     */
    public DataPlacementCostModel(Database catalog_db) {
        this(catalog_db, new PartitionEstimator(catalog_db)); // FIXME
    }
    
    /**
     * I forget why we need this...
     */
    public DataPlacementCostModel(Database catalog_db, PartitionEstimator p_estimator) {
        super(SingleSitedCostModel.class, catalog_db, p_estimator);
    }
    
    @Override
    public AbstractCostModel clone(Database catalogDb) throws CloneNotSupportedException {
        // TODO Auto-generated method stub
        return null;
    }

    /**
     * Takes a TransactionTrace and returns a number refers to the relative 
     * "cost" of executing that txn on a given db design     
     */
    @Override
    public double estimateTransactionCost(Database catalogDb, Workload workload, Filter filter, TransactionTrace xact) throws Exception {
        /** EXAMPLE: txn A needs to touch partitions 1,2,3 and all three of them are on the same node,
         * then the cost is 3. If txn B touches partitions 1 and 4, but 1 and 4 are
         * on difference machines, then the cost is 10. Goal is to tie these numbers with actual
         * runtime behavior that measure with the locality benchmark.
         */
        System.out.println("All partitions touched: " + p_estimator.getAllPartitions(xact));
        // check the sites, host, or partitions that the different partitions belong to
        Map<Integer, Integer> partition_site = new HashMap<Integer, Integer>();
        Map<Integer, Integer> partition_host = new HashMap<Integer, Integer>();
        for (int partition_num : p_estimator.getAllPartitions(xact)) {
            Site site = CatalogUtil.getPartitionById(catalogDb, partition_num).getParent();
            Host host = site.getHost();
            int num_sites_per_host = CatalogUtil.getSitesPerHost(site).get(site.getHost()).size();
            int num_partitions_per_site = site.getPartitions().size();
            System.out.println("num sites per host: " + num_sites_per_host + " num partitions per site: " + num_partitions_per_site);
            int site_num = (int)Math.floor((double)partition_num / (double)num_partitions_per_site);
            int host_num = (int)Math.floor((double)site_num / (double)num_sites_per_host);
            partition_site.put(partition_num, site_num);
            partition_host.put(partition_num, host_num);
        } 
        for (Map.Entry<Integer, Integer> pair : partition_site.entrySet()) {
            System.out.println("partition: " + pair.getKey() + " site: " + pair.getValue());
        }
        for (Map.Entry<Integer, Integer> pair : partition_host.entrySet()) {
            System.out.println("partition: " + pair.getKey() + " host: " + pair.getValue());
        }
        // starting estimating cost        
        return 0;
    }

    @Override
    public void invalidateCache(String catalogKey) {
        // TODO Auto-generated method stub

    }

    @Override
    public void prepareImpl(Database catalogDb) {
        // TODO Auto-generated method stub

    }

}
