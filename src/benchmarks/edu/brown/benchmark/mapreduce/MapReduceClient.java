/***************************************************************************
 *   Copyright (C) 2010 by H-Store Project                                 *
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
package edu.brown.benchmark.mapreduce;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import org.voltdb.TheHashinator;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Site;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.NoConnectionsException;
import org.voltdb.client.ProcedureCallback;

import edu.brown.api.BenchmarkComponent;
import edu.brown.benchmark.mapreduce.MapReduceConstants.ExecutionType;
import edu.brown.catalog.CatalogUtil;
import edu.brown.rand.AbstractRandomGenerator;
import edu.brown.hstore.conf.HStoreConf;

public class MapReduceClient extends BenchmarkComponent {
    
    // --------------------------------------------------------------------
    // DATA MEMBERS
    // --------------------------------------------------------------------

    private ExecutionType m_type = MapReduceConstants.ExecutionType.SAME_SITE;
    protected final AbstractRandomGenerator m_rng;

    /**
     * Number of Records Per Table
     */
    protected final Map<String, Long> table_sizes = new HashMap<String, Long>();

    // --------------------------------------------------------------------
    // TXN PARAMETER GENERATOR
    // --------------------------------------------------------------------
    public interface MapReduceParamGenerator {
        public Object[] generate(AbstractRandomGenerator rng, ExecutionType mtype, Catalog catalog, Map<String, Long> table_sizes);
    }
    
    // --------------------------------------------------------------------
    // BENCHMARK CONTROLLER REQUIREMENTS
    // --------------------------------------------------------------------

    public enum Transaction {
        MockMapReduce(MapReduceConstants.FREQUENCY_MOCK_MAPREDUCE,
            new MapReduceParamGenerator() {
                @Override
                public Object[] generate(AbstractRandomGenerator rng, ExecutionType mtype, Catalog catalog, Map<String, Long> table_sizes) {
                    Object params[] = new Object[0];
                    return (params);
                }
        })
        ;
        
        private Transaction(int weight, MapReduceParamGenerator generator) {
            this.weight = weight;
            this.generator = generator;
            MapReduceClient.TOTAL_WEIGHT += this.weight;
        }
        
        protected static final Map<Integer, Transaction> idx_lookup = new HashMap<Integer, Transaction>();
        static {
            for (Transaction vt : EnumSet.allOf(Transaction.class)) {
                Transaction.idx_lookup.put(vt.ordinal(), vt);
            }
        }
        
        public static Transaction get(int idx) {
            assert(idx >= 0);
            Transaction ret = Transaction.idx_lookup.get(idx);
            return (ret);
        }
        
        public Object[] params(AbstractRandomGenerator rng, ExecutionType mtype, Catalog catalog, Map<String, Long> table_sizes) {
            return (this.generator.generate(rng, mtype, catalog, table_sizes));
        }
        
        public int getWeight() {
            return weight;
        }
        
        private final MapReduceParamGenerator generator;
        private final int weight;
    };
    private static int TOTAL_WEIGHT;
    
    /**
     * For a given a_id, return a new a_id that follows the given scheme of the
     * current ExecutionType for the client.
     * @param a_id
     * @return
     */
    protected static long getDataId(long a_id,  AbstractRandomGenerator rng, ExecutionType type, Catalog catalog, Map<String, Long> table_sizes) {
        long a_id2 = -1;
        Cluster catalog_clus = CatalogUtil.getCluster(catalog);
        long temp = MapReduceConstants.TABLESIZE_TABLEA / catalog_clus.getNum_partitions();
        int num_aids_per_partition = (int)(Math.floor(temp));
        int random_int = rng.nextInt(num_aids_per_partition);
        switch (type) {
            case SAME_PARTITION: {
                int partition_num = TheHashinator.hashToPartition(a_id, catalog_clus.getNum_partitions());
                System.out.println("Total number of partitions: " + catalog_clus.getNum_partitions());
                a_id2 = random_int * catalog_clus.getNum_partitions() + partition_num;
              break;
            }
            case SAME_SITE: {
                int partition_num = TheHashinator.hashToPartition(a_id, catalog_clus.getNum_partitions());
                Site site = CatalogUtil.getPartitionById(catalog, partition_num).getParent();
                int num_sites_per_host = CatalogUtil.getSitesPerHost(site).get(site.getHost()).size();
                int num_partitions_per_site = site.getPartitions().size();
                double a_id_site_num = Math.floor((double)partition_num / (double)num_partitions_per_site);
                double a_id_host_num = Math.floor((double)a_id_site_num / (double)num_sites_per_host);
                //determine the partition range for the cluster (with a random host and random sites)
                //and pick partition randomly from this range
                int lowerbound = (int)a_id_host_num * num_sites_per_host * num_partitions_per_site + (int)a_id_site_num * num_partitions_per_site;
                int upperbound = (int)a_id_host_num * num_sites_per_host * num_partitions_per_site  + (int)a_id_site_num * num_partitions_per_site + (num_partitions_per_site - 1);
                int a_id2_partition_num = rng.numberExcluding(lowerbound, upperbound, partition_num);
                // get a random partition
                a_id2 = random_int * catalog_clus.getNum_partitions() + a_id2_partition_num;
                break;
            }
            case SAME_HOST: {
                int partition_num = TheHashinator.hashToPartition(a_id, catalog_clus.getNum_partitions());
                Site site = CatalogUtil.getPartitionById(catalog, partition_num).getParent();
                int num_sites_per_host = CatalogUtil.getSitesPerHost(site).get(site.getHost()).size();
                int num_partitions_per_site = site.getPartitions().size();

                double a_id_site_num = Math.floor((double)partition_num / (double)num_partitions_per_site);
                double a_id_host_num = Math.floor((double)a_id_site_num / (double)num_sites_per_host);
                int lowerboundsite = (int)a_id_host_num * num_sites_per_host;
                int upperboundsite = (int)a_id_host_num * num_sites_per_host + (num_sites_per_host - 1); 
                int new_site = rng.numberExcluding(lowerboundsite, upperboundsite, (int)a_id_site_num);
                int lowerbound = new_site * num_partitions_per_site;
                int upperbound = new_site * num_partitions_per_site + (num_partitions_per_site - 1);
                int a_id2_partition_num = rng.number(lowerbound, upperbound);
                // get a random partition
                a_id2 = random_int * catalog_clus.getNum_partitions() + a_id2_partition_num;
                break;
            }
            case REMOTE_HOST: {
                int total_number_of_hosts = catalog_clus.getHosts().size();
                int partition_num = TheHashinator.hashToPartition(a_id, catalog_clus.getNum_partitions());
                Site site = CatalogUtil.getPartitionById(catalog, partition_num).getParent();
                int num_sites_per_host = CatalogUtil.getSitesPerHost(site).get(site.getHost()).size();
                int num_partitions_per_site = site.getPartitions().size();
                // get the site number the partition exists on
                double a_id_site_num = Math.floor((double)partition_num / (double)num_partitions_per_site);
                double a_id_host_num = Math.floor((double)a_id_site_num / (double)num_sites_per_host);
                int new_host = (int)a_id_host_num;
                if (total_number_of_hosts > 1)
                {
                    new_host = rng.numberExcluding(0, total_number_of_hosts -1, (int)a_id_host_num);                    
                }
                int new_site = rng.number(0, num_sites_per_host - 1);
                //determine the partition range for the cluster (with a random host and random sites)
                //and pick partition randomly from this range
                int lowerbound = new_host * num_sites_per_host * num_partitions_per_site + new_site * num_partitions_per_site;
                int upperbound = new_host * num_sites_per_host * num_partitions_per_site + new_site * num_partitions_per_site + (num_partitions_per_site - 1);
                int a_id2_partition_num = rng.number(lowerbound, upperbound);
                a_id2 = random_int * catalog_clus.getNum_partitions() + a_id2_partition_num;
                break;
            }
            case RANDOM: {
                a_id2 = rng.nextInt(table_sizes.get(MapReduceConstants.TABLENAME_TABLEA).intValue());
                break;
            }
            default:
                assert(false) : "Unexpected ExecutionType " + type;
        } // SWITCH
        assert(a_id2 != -1);
        return (a_id2);
    }

    /**
     * Transaction Execution Weights
     */
    private static final MapReduceClient.Transaction XACT_WEIGHTS[] = new MapReduceClient.Transaction[100];
    static {
        int i = 0;
        int sum  = 0;
        for (Transaction t : MapReduceClient.Transaction.values()) {
            for (int j = 0; j < t.weight; j++, i++) {
                XACT_WEIGHTS[i] = t;
            } // FOR
            sum += t.weight;
        } // FOR
        assert (100 == sum);
    }
    
    public static void main(String args[]) {
        edu.brown.api.BenchmarkComponent.main(MapReduceClient.class, args, false);
    }

    /**
     * Constructor
     * @param args
     */
    public MapReduceClient(String[] args) {
        super(args);
        assert(this.getCatalogContext() != null);
        // Sanity check
        assert(MapReduceClient.TOTAL_WEIGHT == 100);

        int seed = 0;
        String randGenClassName = RandomGenerator.class.getName();
        String randGenProfilePath = null;

        for (String key : m_extraParams.keySet()) {
            String value = m_extraParams.get(key);

            // Execution Type
            if (key.equalsIgnoreCase("TYPE")) {
                m_type = ExecutionType.valueOf(value);
            }
        } // FOR
        
        AbstractRandomGenerator rng = null;
        try {
            rng = AbstractRandomGenerator.factory(randGenClassName, seed);
            if (randGenProfilePath != null) rng.loadProfile(randGenProfilePath);
        } catch (Exception ex) {
            ex.printStackTrace();
            System.exit(1);
        }
        m_rng = rng;
        
        HStoreConf hstore_conf = this.getHStoreConf();
        
        // Number of Records Per Table
        this.table_sizes.put(MapReduceConstants.TABLENAME_TABLEA, Math.round(MapReduceConstants.TABLESIZE_TABLEA * hstore_conf.client.scalefactor));
        this.table_sizes.put(MapReduceConstants.TABLENAME_TABLEB, Math.round(MapReduceConstants.TABLESIZE_TABLEB * hstore_conf.client.scalefactor));
        for (String tableName : MapReduceConstants.TABLENAMES) {
            assert(this.table_sizes.containsKey(tableName)) : "Missing table size entry for " + tableName;
        } // FOR
    }

    @Override
    public String[] getTransactionDisplayNames() {
        String names[] = new String[Transaction.values().length];
        for (int i = 0; i < names.length; i++) {
            names[i] = Transaction.values()[i].name();
        }
        return names;
    }

    @Override
    public void runLoop() {
        try {
            while (true) {
                runOnce();
                this.getClientHandle().backpressureBarrier();
            } // WHILE
        } catch (NoConnectionsException e) {
            /*
             * Client has no clean mechanism for terminating with the DB.
             */
            return;
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (IOException e) {
            /*
             * At shutdown an IOException is thrown for every connection to
             * the DB that is lost Ignore the exception here in order to not
             * get spammed, but will miss lost connections at runtime
             */
        }
    }
    
    @Override
    protected boolean runOnce() throws IOException {
        MapReduceClient.Transaction txn_type = XACT_WEIGHTS[m_rng.number(0, 99)];
        assert(txn_type != null);
        Object params[] = txn_type.params(m_rng,m_type, this.getCatalogContext().catalog, this.table_sizes);
        boolean ret = this.getClientHandle().callProcedure(new MapReduceCallback(txn_type), txn_type.name(), params);
        return (ret);
    }
    
    /**
     * Basic Callback Class
     */
    protected class MapReduceCallback implements ProcedureCallback {
        private final Transaction txn;
        
        public MapReduceCallback(Transaction txn) {
            super();
            this.txn = txn;
        }
        
        @Override
        public void clientCallback(ClientResponse clientResponse) {
            System.out.println(Arrays.toString(clientResponse.getResults()));
            incrementTransactionCounter(clientResponse, this.txn.ordinal());
        }
    } // END CLASS
}
