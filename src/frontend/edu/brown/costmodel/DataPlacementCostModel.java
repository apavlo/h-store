package edu.brown.costmodel;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.types.QueryType;

import sun.tools.attach.HotSpotVirtualMachine;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.costmodel.MarkovCostModel.Penalty;
import edu.brown.costmodel.MarkovCostModel.PenaltyGroup;
import edu.brown.costmodel.SingleSitedCostModel.QueryCacheEntry;
import edu.brown.costmodel.SingleSitedCostModel.TransactionCacheEntry;
import edu.brown.utils.PartitionEstimator;
import edu.brown.workload.QueryTrace;
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
    private static final Logger LOG = Logger.getLogger(DataPlacementCostModel.class);
    private final CachedPartitionEstimator cache_estimator;

    
    public enum PenaltyGroup {
        DIFFERENT_PARTITION,
        DIFFERENCE_SITE,
        DIFFERENT_HOST;
    }
    
    /**
     * Cost Model Penalties
     */
    public enum Penalty {
        // ----------------------------------------------------------------------------
        // PENALTY #1
        // ----------------------------------------------------------------------------
        READ_DIFFERENT_PARTITION             (PenaltyGroup.DIFFERENT_PARTITION, 0.1d),
        WRITE_DIFFERENT_PARTITION             (PenaltyGroup.DIFFERENT_PARTITION, 0.1d),
        // ----------------------------------------------------------------------------
        // PENALTY #2
        // ----------------------------------------------------------------------------
        READ_DIFFERENT_SITE             (PenaltyGroup.DIFFERENCE_SITE, 0.2d),
        WRITE_DIFFERENT_SITE             (PenaltyGroup.DIFFERENCE_SITE, 0.2d),
        // ----------------------------------------------------------------------------
        // PENALTY #3
        // ----------------------------------------------------------------------------
        READ_DIFFERENT_HOST             (PenaltyGroup.DIFFERENT_HOST, 0.5d),
        WRITE_DIFFERENT_HOST             (PenaltyGroup.DIFFERENT_HOST, 0.5d),
        ;
        private final double cost;
        private final PenaltyGroup group;
        private Penalty(PenaltyGroup group, double cost) {
            this.group = group;
            this.cost = cost;
        }
        public double getCost() {
            return this.cost;
        }
        public PenaltyGroup getGroup() {
            return this.group;
        }
    }
    
    private class CachedPartitionEstimator extends PartitionEstimator {
        private final PartitionEstimator p_estimator;
        private final Database catalog_db;
        private final Map<QueryTrace, Set<Integer>> cache = new HashMap<QueryTrace, Set<Integer>>();
        
        public CachedPartitionEstimator(PartitionEstimator p_estimator, Database catalog_db) {
            super(p_estimator.getDatabase(), p_estimator.getHasher());
            this.catalog_db = catalog_db;
            this.p_estimator = p_estimator;
        }
        
        public Set<Integer> getAllPartitions(final QueryTrace query, Integer base_partition) throws Exception {
            Set<Integer> ret = this.cache.get(query);
            if (ret == null) {
                ret = this.p_estimator.getAllPartitions(query, base_partition);
                for (Integer partition_num : ret) {
                    Partition p = new Partition();
                    LOG.info("partition objects: " + partition_num + " host: " + ((Site)CatalogUtil.getPartitionById(catalog_db, partition_num).getParent()).getHost());
                }
                this.cache.put(query, ret);
            }
            return (ret);
        }
    }

    
    
    /** maps a partition to its site and host **/
    Map<Integer, Object[]> partition_site_host = new HashMap<Integer, Object[]>();
    
    /** stores all the penalities **/
    List<Penalty> penalties = new ArrayList<Penalty>();
    
    
    final Set<Integer> read_partitions = new HashSet<Integer>();
    final Set<Integer> write_partitions = new HashSet<Integer>();
    
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
        super(DataPlacementCostModel.class, catalog_db, p_estimator);
        cache_estimator = new CachedPartitionEstimator(p_estimator, catalog_db);
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
        //this.prepareImpl(catalogDb);
        //LOG.info("All partitions touched estimateTransactionCost: " + p_estimator.getAllPartitions(xact));
        int base_partition = p_estimator.getBasePartition(xact);
        Site base_site = CatalogUtil.getPartitionById(catalogDb, base_partition).getParent();
        Host base_host = base_site.getHost();
        LOG.info("base partition: " + base_partition);
        LOG.info("batch count: " + xact.getBatchCount());
        
        // For each batch, get the set of partitions that the queries in the batch touch
        
        for (int batch_id : xact.getBatchIds()) {
            read_partitions.clear();
            write_partitions.clear();
            for (QueryTrace query : xact.getBatchQueries(batch_id)) {
                if (QueryType.SELECT.getValue() == query.getCatalogItem(catalogDb).getQuerytype()) {
                    read_partitions.addAll(cache_estimator.getAllPartitions(query, base_partition));
                } else {
                    write_partitions.addAll(cache_estimator.getAllPartitions(query, base_partition));
                }
            } // FOR
            for (Integer partition_id : read_partitions) {
                if (partition_id != base_partition) {
                    Site current_partition_site = CatalogUtil.getPartitionById(catalogDb, base_partition).getParent();
                    if (current_partition_site == base_site) {
                        // different partitions, same site
                        penalties.add(Penalty.READ_DIFFERENT_PARTITION);
                    } else if (current_partition_site.getHost() == base_host) {
                        // different sites, same host
                        penalties.add(Penalty.READ_DIFFERENT_PARTITION);
                    } else {
                        // different hosts
                        penalties.add(Penalty.READ_DIFFERENT_HOST);
                    }
                }                
            }
            for (Integer partition_id : write_partitions) {
                if (partition_id != base_partition) {
                    Site current_partition_site = CatalogUtil.getPartitionById(catalogDb, base_partition).getParent();
                    if (current_partition_site == base_site) {
                        // different partitions, same site
                        penalties.add(Penalty.WRITE_DIFFERENT_PARTITION);
                    } else if (current_partition_site.getHost() == base_host) {
                        // different sites, same host
                        penalties.add(Penalty.WRITE_DIFFERENT_SITE);
                    } else {
                        // different hosts
                        penalties.add(Penalty.WRITE_DIFFERENT_HOST);
                    }
                }                                    
            }
        }
        double cost = 0.0d;
        LOG.info("penalties: " + this.penalties);
        for (Penalty p : this.penalties) cost += p.getCost();
        LOG.info("cost for transactions: " + xact.getCatalogItemName() + " is: " + cost);
        return (cost);
    }

    @Override
    public void invalidateCache(String catalogKey) {
        // TODO Auto-generated method stub

    }

    @Override
    public void prepareImpl(Database catalogDb) {
        // FIXME: Create all host<->site<->partition maps
        this.p_estimator.initCatalog(catalogDb);
        LOG.info("All partitions prepareImpl: " + CatalogUtil.getAllPartitionIds(catalogDb.getCatalog()));
        // check the sites, host, or partitions that the different partitions belong to
        for (int partition_num : CatalogUtil.getAllPartitionIds(catalogDb.getCatalog())) {
            Partition catalog_part = CatalogUtil.getPartitionById(catalogDb, partition_num); 
            Site site = catalog_part.getParent();
            // ID->site.getId();
            Host host = site.getHost();
            // ID->host.getName();
            LOG.debug("hostname: " + host.getName());
            Object[] site_host = new Object[2];
            site_host[0] = site.getId();
            site_host[1] = host.getName();
            partition_site_host.put(partition_num, site_host);
        } 
        for (Integer partition : partition_site_host.keySet()) {
            LOG.debug("partition: " + partition   + " site: " + partition_site_host.get(partition)[0] + " host: " + partition_site_host.get(partition)[1]);
        }
    }

}
