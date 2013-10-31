/**
 * 
 */
package edu.brown.designer.mappers;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.voltdb.catalog.Table;
import org.voltdb.utils.Pair;

import edu.brown.catalog.CatalogKey;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.DefaultHasher;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.workload.AbstractTraceElement;
import edu.brown.workload.TransactionTrace;

/**
 * @author pavlo
 */
public class AffinityMapper extends AbstractMapper {

    private final Map<Pair<Table, Table>, FragmentAffinity> partition_histograms = new HashMap<Pair<Table, Table>, FragmentAffinity>();
    private final Map<Table, Set<FragmentAffinity>> table_histogram_xref = new HashMap<Table, Set<FragmentAffinity>>();

    protected class FragmentAffinity {
        private final Table root;

        /**
         * Nasty! <root0.PartitionId, root1.PartitionId> -> Histogram
         */
        private final Map<Integer, ObjectHistogram> histograms = new HashMap<Integer, ObjectHistogram>();

        public FragmentAffinity(Table root) {
            this.root = root;
        }

        public Table getRootTable() {
            return (this.root);
        }

        public void add(int hash0, int hash1) {
            if (!this.histograms.containsKey(hash0)) {
                this.histograms.put(hash0, new ObjectHistogram());
            }
            this.histograms.get(hash0).put(hash1);
        }

        @Override
        public String toString() {
            StringBuilder buffer = new StringBuilder();
            for (Integer partition : this.histograms.keySet()) {
                buffer.append("Partition: ").append(partition).append("\n");
                buffer.append(this.histograms.get(partition));
                buffer.append("--------------------------------------\n");
            } // FOR
            return (buffer.toString());
        }
    }

    public AffinityMapper(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    @Override
    public PartitionMapping generate(DesignerHints hints, PartitionPlan pplan) throws Exception {
        //
        // Unfortunately at this point we have to run this through again in
        // order to figure
        // out where multi-site transactions are going.
        //
        SingleSitedCostModel cost_model = new SingleSitedCostModel(info.catalogContext);
        LOG.info("Generating cost model information for given PartitionPlan");
        cost_model.estimateWorkloadCost(info.catalogContext, this.info.workload);

        int num_partitions = info.catalogContext.numberOfPartitions;
        AbstractHasher hasher = new DefaultHasher(info.catalogContext, num_partitions);

        Collection<Table> roots = pplan.getNonReplicatedRoots();
        Map<Table, List<Integer>> table_partition_values = new HashMap<Table, List<Integer>>();
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            table_partition_values.put(catalog_tbl, new ArrayList<Integer>());
            if (roots.contains(catalog_tbl)) {
                this.table_histogram_xref.put(catalog_tbl, new HashSet<FragmentAffinity>());
            }
        } // FOR

        // ----------------------------------------------------------
        // Affinity Histograms
        // ----------------------------------------------------------

        //
        // Construct a mapping from pairs of roots to a histogram
        // This allows us to keep track of
        //
        for (Table root0 : roots) {
            for (Table root1 : roots) {
                Pair<Table, Table> pair = new Pair<Table, Table>(root0, root1);
                FragmentAffinity affinity0 = new FragmentAffinity(root0);
                this.partition_histograms.put(pair, affinity0);
                this.table_histogram_xref.get(root0).add(affinity0);

                if (!root0.equals(root1)) {
                    pair = new Pair<Table, Table>(root1, root0);
                    FragmentAffinity affinity1 = new FragmentAffinity(root1);
                    this.partition_histograms.put(pair, new FragmentAffinity(root1));
                    this.table_histogram_xref.get(root1).add(affinity1);
                }
            } // FOR
        } // FOR

        //
        // For each transaction that is not single-sited, figure out what
        // partitions
        // that it wants to go to
        //
        LOG.info("Generating affinity information for roots " + roots);
        Set<TransactionTrace> multisite_xacts = new HashSet<TransactionTrace>();
        int xact_ctr = 0;
        for (AbstractTraceElement<?> element : this.info.workload) {
            if (!(element instanceof TransactionTrace))
                continue;
            TransactionTrace xact = (TransactionTrace) element;
            SingleSitedCostModel.TransactionCacheEntry xact_cost = null; // FIXME
                                                                         // cost_model.getTransactionCacheEntry(xact);
            if (xact_cost.isSinglePartitioned())
                continue;
            multisite_xacts.add(xact);

            // System.out.println(xact + "\n" + xact_cost);

            for (List<Integer> values : table_partition_values.values()) {
                values.clear();
            } // FOR

            //
            // There are two types of multi-site transactions:
            // (1) All of the individual queries are ss, but the total xact is
            // ms
            // (2) One or more of the queries are multi-sited
            // A transaction may be a combination of both of these.
            //
            // Furthermore, a ms xact may be multi-sited because it either:
            // (1) Uses data in separate partition trees (at this point we don't
            // know whether
            // the fragments from the different trees are on the same site)
            // (2) Use data from the same partition tree, but based on different
            // values
            // of the partitioning attribute.
            //
            // So now we need to look at each query and determine what fragments
            // they
            // want to go at.
            //

            for (SingleSitedCostModel.QueryCacheEntry query_cost : cost_model.getQueryCacheEntries(xact)) {
                // QueryTrace query =
                // (QueryTrace)info.workload.getTraceObject(query_cost.getQueryId());
                // assert(query != null);
                // System.out.println(query + "\n" + query_cost + "\n");
                //
                // For each table, get the values used for the partition keys
                //
                for (String table_key : query_cost.getTableKeys()) {
                    Table catalog_tbl = CatalogKey.getFromKey(info.catalogContext.database, table_key, Table.class);
                    assert (catalog_tbl != null);
                    Table root = pplan.getRoot(catalog_tbl);
                    assert (root != null);

                    // for (Object value : query_cost.getPartitions(table_key))
                    // {
                    // int hash = hasher.hash(value, catalog_tbl);
                    // table_partition_values.get(root).add(hash);
                    // } // FOR
                } // FOR
            } // FOR

            //
            // Now figure out what slice of the partition tree each table
            // belongs to and then
            // update the histograms between the two root tables.
            //
            for (Table root0 : roots) {
                List<Integer> hashes0 = table_partition_values.get(root0);
                if (hashes0.isEmpty())
                    continue;

                for (Table root1 : roots) {
                    List<Integer> hashes1 = table_partition_values.get(root1);
                    if (hashes1.isEmpty())
                        continue;

                    boolean same_root = root0.equals(root1);
                    FragmentAffinity affinity0 = this.getFragmentAffinity(root0, root1);
                    FragmentAffinity affinity1 = (!same_root ? this.getFragmentAffinity(root1, root0) : null);

                    // System.out.println("ROOTS: " + root0 + " <-> " + root1);

                    for (Integer hash0 : hashes0) {
                        for (Integer hash1 : hashes1) {
                            if (hash0 != hash1 || (hash0 == hash1 && !same_root)) {
                                affinity0.add(hash0, hash1);
                                if (affinity1 != null)
                                    affinity1.add(hash1, hash0);
                            }
                        } // FOR (hash1)
                    } // FOR (hash0)
                } // FOR (root1)
            } // FOR (root0)
            xact_ctr++;
            if (xact_ctr % 100 == 0)
                LOG.info("Processed affinity for " + xact_ctr + " transactions...");
        } // FOR (xact)
        for (Pair<Table, Table> pair : this.partition_histograms.keySet()) {
            System.out.println(pair);
            System.out.println(this.partition_histograms.get(pair));
            System.out.println("===============================================");
        }
        // System.exit(1);

        // ----------------------------------------------------------
        // Initial Solution
        // ----------------------------------------------------------
        PartitionMapping pmap = new SimpleMapper(this.designer, this.info).generate(hints, pplan);
        pmap.apply(info.catalogContext.database, info.stats, hasher);
        System.out.println(pmap);

        return null;
    }

    public FragmentAffinity getFragmentAffinity(Table root0, Table root1) {
        if (root0.compareTo(root1) > 0) {
            Table temp = root0;
            root0 = root1;
            root1 = temp;
        }
        Pair<Table, Table> pair = new Pair<Table, Table>(root0, root1);
        // System.out.println("MY PAIR: " + pair + " [" + pair.hashCode() +
        // "]");
        // int ctr = 0;
        // for (Pair<Table, Table> other : this.partition_histograms.keySet()) {
        // System.out.println("[" + ctr++ + "]: " + other + " [" +
        // other.hashCode() + "]");
        // }
        // System.out.flush();
        return (this.partition_histograms.get(pair));
    }

}
