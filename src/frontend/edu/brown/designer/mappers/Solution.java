package edu.brown.designer.mappers;

import java.util.SortedSet;
import java.util.TreeSet;

import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Database;

import edu.brown.graphs.AbstractDirectedTree;
import edu.brown.graphs.AbstractEdge;
import edu.brown.graphs.AbstractVertex;
import edu.brown.hashing.AbstractHasher;
import edu.brown.hashing.MappedHasher;

public class Solution extends AbstractDirectedTree<Solution.Vertex, Solution.Edge> {
    private static final long serialVersionUID = 1L;
    private Double cost = null;
    private final SortedSet<Integer> partitions = new TreeSet<Integer>();

    /**
     * Edge
     */
    public class Edge extends AbstractEdge {
        public Edge() {
            super(Solution.this);
        }
    }

    /**
     * Vertex
     */
    public class Vertex extends AbstractVertex {
        private int num_children;

        public Vertex(CatalogType catalog_item) {
            super();

            //
            // Based on the object type, get the number of children elements
            // that this object is allowed to have in the tree
            //
//            if (catalog_item instanceof HardwareCPU) {
//                this.num_children = HardwareCatalogUtil.getCoresPerCPU(catalog_item);
//            } else if (catalog_item instanceof HardwareCore) {
//                this.num_children = HardwareCatalogUtil.getThreadsPerCore(catalog_item);
//            } else if (catalog_item instanceof HardwareCore) {
//                this.num_children = Integer.MAX_VALUE;
//            } else if (catalog_item instanceof Host) {
//                this.num_children = ((Host) catalog_item).getNum_cpus();
//            } else {
//                this.num_children = 0;
//                assert (false) : "Unexpected catalog item " + catalog_item;
//            }
        }

        public int getFreeSlots() {
            return (this.num_children - Solution.this.getChildCount(this));
        }

        public int getNumChildren() {
            return (this.num_children);
        }

        public Solution.Vertex getParent() {
            return (Solution.this.getParent(this));
        }
    } // CLASS

    /**
     * Constructor
     * 
     * @param catalog_db
     */
    public Solution(Database catalog_db) {
        super(catalog_db);
        this.init();
    }

    /**
     * Initialize the graph structure for the catalog elements
     */
    private void init() {
        Cluster catalog_cluster = (Cluster) this.getDatabase().getParent();

//        // Hosts
//        for (Host catalog_host : catalog_cluster.getHosts()) {
//            Solution.Vertex v_host = new Solution.Vertex(catalog_host);
//            // CPUS
//            for (HardwareCPU catalog_cpu : catalog_host.getCpus()) {
//                Solution.Vertex v_cpu = new Solution.Vertex(catalog_cpu);
//                this.addEdge(new Solution.Edge(), v_host, v_cpu);
//                // Cores
//                for (HardwareCore catalog_core : catalog_cpu.getCores()) {
//                    Solution.Vertex v_core = new Solution.Vertex(catalog_core);
//                    this.addEdge(new Solution.Edge(), v_cpu, v_core);
//                    // Threads
//                    for (HardwareThread catalog_thread : catalog_core.getThreads()) {
//                        Solution.Vertex v_thread = new Solution.Vertex(catalog_thread);
//                        this.addEdge(new Solution.Edge(), v_core, v_thread);
//                    } // FOR
//                } // FOR
//            } // FOR
//        } // FOR
        return;
    }

    @Override
    public boolean addEdge(Edge e, Vertex v1, Vertex v2) {
//        // Host -> HardwareCPU
//        if (v1.getCatalogItem() instanceof Host) {
//            assert (v2.getCatalogItem() instanceof HardwareCPU);
//            // HardwareCPU -> HardwareCore
//        } else if (v1.getCatalogItem() instanceof HardwareCPU) {
//            assert (v2.getCatalogItem() instanceof HardwareCore);
//            // HardwareCore -> HardwareThread
//        } else if (v1.getCatalogItem() instanceof HardwareCore) {
//            assert (v2.getCatalogItem() instanceof HardwareThread);
//            // HardwareThread -> Partition
//        } else if (v1.getCatalogItem() instanceof HardwareThread) {
//            assert (v2.getCatalogItem() instanceof Partition);
//            // Bad mojo!
//        } else {
//            assert (false) : "Unexpected catalog item for first vertex " + v1.getCatalogItem();
//        }
        return super.addEdge(e, v1, v2);
    }

    public Double getCost() {
        return this.cost;
    }

    public void setCost(Double cost) {
        this.cost = cost;
    }

//    public void addPartition(HardwareThread catalog_thread, Integer part_id) {
//        assert (catalog_thread.getPartitions().get(part_id.toString()) == null);
//
//        Partition catalog_part = catalog_thread.getPartitions().add(part_id.toString());
//        assert (catalog_part != null);
//
//        Solution.Vertex v_thread = this.getVertex(catalog_thread);
//        assert (v_thread != null);
//        Solution.Vertex v_part = new Solution.Vertex(catalog_part);
//        assert (v_part != null);
//
//        this.addEdge(new Solution.Edge(), v_thread, v_part);
//        this.partitions.add(part_id);
//    }

    public boolean hasPartition(int part_id) {
        return (this.partitions.contains(part_id));
    }

    // @Override
    // public String toString() {
    // String ret = "Solution [Cost=" + this.cost + "]:\n";
    // for (Integer id : this.keySet()) {
    // ret += "   Node" + id + " -> " + this.get(id) + "\n";
    // } // FOR
    // return (ret);
    // }
    public AbstractHasher toHasher() {
        // Get the number of partitions
        // int num_partitions = 0;
        // for (Node node : this.values()) {
        // num_partitions += node.size();
        // } // FOR

        MappedHasher hasher = new MappedHasher(null, this.partitions.size());
        // DefaultHasher default_hasher = new DefaultHasher(null,
        // num_partitions);
        // int partition_id = 0;
        // for (Node node : this.values()) {
        // for (Integer id : node) {
        // hasher.map(default_hasher.hash(id), partition_id++);
        // } // FOR
        // } // FOR
        return (hasher);
    }

}
