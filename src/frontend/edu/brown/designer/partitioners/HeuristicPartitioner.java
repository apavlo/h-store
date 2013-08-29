package edu.brown.designer.partitioners;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.voltdb.CatalogContext;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

import edu.brown.catalog.CatalogCloner;
import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.costmodel.SingleSitedCostModel;
import edu.brown.costmodel.TimeIntervalCostModel;
import edu.brown.designer.AccessGraph;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.DesignerVertex;
import edu.brown.designer.PartitionTree;
import edu.brown.designer.generators.ReplicationTreeGenerator;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.graphs.AbstractDirectedGraph;
import edu.brown.graphs.IGraph;
import edu.brown.graphs.VertexTreeWalker;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;
import edu.brown.utils.ThreadUtil;

@Deprecated
public class HeuristicPartitioner extends AbstractPartitioner {
    protected static final Logger LOG = Logger.getLogger(HeuristicPartitioner.class);

    // ----------------------------------------------------
    // DATA MEMBERS
    // ----------------------------------------------------

    /**
     * We use a separate cost model instance for each of time buckets
     */
    protected final TimeIntervalCostModel<SingleSitedCostModel> cost_model;

    /**
     * Used to sort vertices when constructing a PartitionTree
     * 
     * @author pavlo
     */
    protected class VertexComparator implements Comparator<DesignerVertex> {
        private final IGraph<DesignerVertex, DesignerEdge> graph;
        private final DesignerVertex parent;
        private final Map<DesignerVertex, Double> cache = new HashMap<DesignerVertex, Double>();

        public VertexComparator(IGraph<DesignerVertex, DesignerEdge> graph, DesignerVertex parent) {
            this.graph = graph;
            this.parent = parent;
        }

        /*
         * @Override public int compare(Vertex v0, Vertex v1) { Collection<Edge>
         * edges0 = this.graph.getIncidentEdges(v0); Collection<Edge> edges1 =
         * this.graph.getIncidentEdges(v1); int weight0 = (edges0 != null ?
         * edges0.size() : 0); int weight1 = (edges1 != null ? edges1.size() :
         * 0); int diff = weight1 - weight0; if (diff == 0) diff =
         * v0.getTable().getGuid() - v1.getTable().getGuid(); return (diff); }
         */

        private double getWeight(DesignerVertex v) {
            double weight = 0;
            if (this.cache.containsKey(v)) {
                weight = this.cache.get(v);
            } else {
                // Take the sum of all the edge weights for all time buckets
                for (DesignerEdge edge : this.graph.findEdgeSet(this.parent, v)) {
                    weight += edge.getTotalWeight();
                } // FOR
                this.cache.put(v, weight);
            }
            return (weight);
        }

        @Override
        public int compare(DesignerVertex v0, DesignerVertex v1) {
            double weight0 = this.getWeight(v0);
            double weight1 = this.getWeight(v1);
            int diff = (int) Math.round(weight1 - weight0);
            if (diff == 0)
                diff = v0.getCatalogItem().compareTo(v1.getCatalogItem());
            return (diff);
        }
    } // END CLASS

    // ----------------------------------------------------
    // CONSTRUCTORS
    // ----------------------------------------------------

    /**
     * Constructor
     * 
     * @param designer
     * @param info
     */
    public HeuristicPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);

        // Initialize Cost Model
        assert (info.getNumIntervals() > 0);
        this.cost_model = new TimeIntervalCostModel<SingleSitedCostModel>(info.catalogContext, SingleSitedCostModel.class, info.getNumIntervals());
    }

    // ----------------------------------------------------
    // METHODS
    // ----------------------------------------------------

    @Override
    public PartitionPlan generate(final DesignerHints hints) throws Exception {

        // ----------------------------------------------------
        // (1) Fork a new thread to generate a partition tree for each stored
        // procedure
        // ----------------------------------------------------
        final Vector<PartitionTree> partition_trees = new Vector<PartitionTree>();
        List<Thread> threads = new ArrayList<Thread>();
        for (final Procedure catalog_proc : info.catalogContext.database.getProcedures()) {
            final AccessGraph agraph = designer.getAccessGraph(catalog_proc);
            if (agraph == null)
                continue;
            LOG.debug("Creating a new thread for contructing a PartitionForest for " + catalog_proc);
            Thread thread = new Thread() {
                @Override
                public String toString() {
                    return catalog_proc.getName() + "-" + super.toString();
                }

                @Override
                public void run() {
                    try {
                        partition_trees.add(HeuristicPartitioner.this.generatePartitionTree(catalog_proc, agraph, hints));
                    } catch (Exception ex) {
                        LOG.fatal("Failed to create PartitionTree for " + catalog_proc, ex);
                        System.exit(1);
                    }
                }
            }; // THREAD
            threads.add(thread);
        } // FOR
        ThreadUtil.runNewPool(threads);

        // ----------------------------------------------------
        // (2) Combine all of the PartitionTrees from each Procedure into single
        // partition tree that we can use to generate the PartitionMapping table
        // ----------------------------------------------------
        PartitionPlan pplan = null;
        if (!partition_trees.isEmpty()) {
            LOG.debug("Generating final partition plan using " + partition_trees.size() + " PartitionTrees");
            pplan = this.generatePlan(partition_trees);
        }
        return (pplan);
    }

    /**
     * Generate a PartitionTree for a single Procedure
     * 
     * @param catalog_proc
     * @param agraph
     * @param hints
     * @return
     * @throws Exception
     */
    protected PartitionTree generatePartitionTree(Procedure catalog_proc, AccessGraph agraph, DesignerHints hints) throws Exception {
        LOG.debug("Creating PartitionTree for " + catalog_proc);
        WorkloadFilter filter = null;
        Collection<Table> proc_tables = null;
        try {
            filter = new WorkloadFilter(CatalogUtil.getDatabase(catalog_proc), catalog_proc);
            proc_tables = CatalogUtil.getReferencedTables(catalog_proc);
        } catch (Exception ex) {
            LOG.fatal(ex.getLocalizedMessage());
            ex.printStackTrace();
            System.exit(1);
        }
        double overall_best_cost = Double.MAX_VALUE;
        DesignerHints proc_hints = hints.clone();

        // We take multiple passes through the partition trees until we come up
        // with one
        // that makes our procedure single-sited
        int round = -1;
        int round_limit = 4;
        PartitionTree ptree = null;
        while (true) {
            if (++round > round_limit)
                break;

            // Get the list of candidate roots and create a partition tree
            List<DesignerVertex> candidate_roots = null;
            try {
                candidate_roots = this.createCandidateRoots(proc_hints, agraph);
            } catch (Exception ex) {
                LOG.fatal(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }
            ptree = new PartitionTree(info.catalogContext.database);
            ptree.setName("PartTree-Round" + round);
            //
            // Make sure we include the replicated tables
            //
            for (String table_name : proc_hints.force_replication) {
                DesignerVertex vertex = agraph.getVertex(info.catalogContext.database.getTables().get(table_name));
                ptree.addVertex(vertex);
                vertex.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.REPLICATION);
            } // FOR
            try {
                for (DesignerVertex root : candidate_roots) {
                    buildPartitionTree(ptree, root, agraph, proc_hints);
                } // FOR
            } catch (Exception ex) {
                LOG.fatal(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }

            //
            // We add the procedure that was used to generate this ptree for
            // debugging
            // Weight the partition trees by how often the procedure is executed
            //
            ptree.getProcedures().add(catalog_proc);
            ptree.setWeight((double) info.workload.getTraces(catalog_proc).size());
            designer.getGraphs(catalog_proc).add(ptree);

            //
            // Now go through and see if there any tables that need to be
            // replicated
            //
            LOG.debug("Invoking replication tree generation...");

            AbstractDirectedGraph<DesignerVertex, DesignerEdge> rtree = new AbstractDirectedGraph<DesignerVertex, DesignerEdge>(info.catalogContext.database) {
                private static final long serialVersionUID = 1L;
            };
            rtree.setName("RepTree-Round" + round);
            ReplicationTreeGenerator replication_check = new ReplicationTreeGenerator(info, agraph, ptree);
            try {
                replication_check.generate(rtree);
            } catch (Exception ex) {
                LOG.fatal(ex.getLocalizedMessage());
                ex.printStackTrace();
                System.exit(1);
            }
            designer.getGraphs(catalog_proc).add(rtree);

            //
            // If there are no tables that are conflicted, then there is nothing
            // else we can do
            //
            if (replication_check.getConflictVertices().isEmpty())
                break;

            //
            // Examine the edges that we created and see if there is a common
            // ancestor that the
            // the destination vertex wants to be partitioned on
            //
            List<Table> candidates = new ArrayList<Table>();
            candidates.addAll(replication_check.getReplicationCandidates());
            boolean forced_dependency = false;
            for (DesignerVertex conflict_vertex : replication_check.getConflictVertices()) {
                Table conflict_tbl = conflict_vertex.getCatalogItem();
                //
                // For each edge in the replication tree that is coming into
                // this conflict vertex,
                // see whether there is an overlapping ancestor
                //
                Map<Column, Integer> ancestors = new HashMap<Column, Integer>();
                int max_count = 0;
                Column max_column = null;
                Column max_conflict_column = null;
                for (DesignerEdge conflict_edge : rtree.getInEdges(conflict_vertex)) {
                    PredicatePairs cset = (PredicatePairs) conflict_edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
                    for (Column conflict_column : cset.findAllForParent(Column.class, conflict_tbl)) {
                        Column ancestor_column = CollectionUtil.last(info.dependencies.getAncestors(conflict_column));
                        Integer count = ancestors.get(ancestor_column);
                        count = (count == null ? 1 : count + 1);
                        ancestors.put(ancestor_column, count);
                        if (count > max_count) {
                            max_count = count;
                            max_column = ancestor_column;
                            max_conflict_column = conflict_column;
                        }
                    } // FOR
                } // FOR
                assert (max_column != null);
                //
                // If we have a column that is used in both trees, then that's
                // the one we'll want to partition
                // our buddy on...
                //
                boolean valid = true;
                for (Column column : ancestors.keySet()) {
                    if (!max_column.equals(column) && max_count == ancestors.get(column)) {
                        valid = false;
                        break;
                    }
                } // FOR
                LOG.debug("CONFLICT - " + conflict_vertex + ": " + ancestors);
                if (valid) {
                    String child_key = CatalogKey.createKey(max_conflict_column.getParent());
                    String parent_key = CatalogKey.createKey(max_column.getParent());
                    String orig_parent_key = proc_hints.force_dependency.put(child_key, parent_key);
                    if (!parent_key.equals(orig_parent_key)) {
                        LOG.debug("Forcing dependency " + child_key + "->" + parent_key);
                        forced_dependency = true;
                    }
                }
            } // FOR
            if (forced_dependency)
                continue;

            //
            // Now for each candidate we need to check whether replicating would
            // actually improve
            // the performance of this procedure. So first we need to get a
            // baseline cost
            // of the procedure on the workload
            // TODO: Support set sizes greater than 2!!
            //
            Set<Set<Table>> candidate_sets = new LinkedHashSet<Set<Table>>();
            assert (candidates.size() <= 2);
            for (int ctr0 = 0, cnt = candidates.size(); ctr0 < cnt; ctr0++) {
                Set<Table> set = new HashSet<Table>();
                set.add(candidates.get(ctr0));
                candidate_sets.add(set);
                for (int ctr1 = ctr0 + 1; ctr1 < cnt; ctr1++) {
                    set = new HashSet<Table>();
                    set.add(candidates.get(ctr0));
                    set.add(candidates.get(ctr1));
                    candidate_sets.add(set);
                } // FOR
            } // FOR

            //
            // Calculate the cost of each replication candidate set
            // If this is the first time we are doing this for this procedure,
            // then add in a blank
            // replication set so that we can get the baseline cost
            //
            Set<Table> best_set = null;
            double best_cost = Double.MAX_VALUE;
            if (best_cost == overall_best_cost)
                candidate_sets.add(new HashSet<Table>());
            for (Set<Table> replication_set : candidate_sets) {
                cost_model.invalidateCache(candidates);

                Catalog new_catalog = CatalogCloner.cloneBaseCatalog(info.catalogContext.database.getCatalog());
                for (Table catalog_tbl : proc_tables) {
                    DesignerVertex vertex = ptree.getVertex(catalog_tbl);
                    assert (vertex != null) : "PartitionTree is missing a vertex for " + catalog_tbl + " " + ptree.getVertices();
                    Table new_catalog_tbl = CatalogCloner.clone(catalog_tbl, new_catalog);

                    //
                    // Mark the table as replicated if it's in the current set
                    //
                    if (replication_set.contains(catalog_tbl) || ptree.isReplicated(vertex)) {
                        new_catalog_tbl.setIsreplicated(true);
                        new_catalog_tbl.setPartitioncolumn(ReplicatedColumn.get(new_catalog_tbl));
                        //
                        // Otherwise partition it according to the current
                        // partition tree
                        //
                    } else {
                        Column catalog_col = (Column) vertex.getAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name());
                        assert (catalog_col != null) : "Failed to retrieve partition column for " + catalog_tbl + " " + vertex.getAttributeValues(ptree);
                        Column new_catalog_col = new_catalog_tbl.getColumns().get(catalog_col.getName());
                        assert (new_catalog_col != null);
                        new_catalog_tbl.setIsreplicated(false);
                        new_catalog_tbl.setPartitioncolumn(new_catalog_col);
                    }
                } // FOR
                Database new_catalog_db = CatalogUtil.getDatabase(new_catalog);
                CatalogCloner.cloneConstraints(info.catalogContext.database, new_catalog_db);
                CatalogContext newCatalogContext = new CatalogContext(new_catalog);

                double cost = 0d;
                try {
                    cost_model.estimateWorkloadCost(newCatalogContext, info.workload, filter, null);
                } catch (Exception ex) {
                    LOG.fatal(ex.getLocalizedMessage());
                    ex.printStackTrace();
                    System.exit(1);
                }
                LOG.debug(replication_set + ": " + cost);
                if (cost <= best_cost) {
                    //
                    // Always choose the smaller set
                    //
                    if (best_set != null && cost == best_cost && best_set.size() < replication_set.size())
                        continue;
                    best_set = replication_set;
                    best_cost = cost;
                }
            } // FOR
            assert (best_set != null);

            //
            // Now this part is important!
            // If the overall_best_cost is equal to the best cost, then we can
            // go
            // ahead and replicate these mofos!
            //
            if (best_cost < overall_best_cost) {
                overall_best_cost = best_cost;
                LOG.debug("Marking tables as replicated: " + best_set);
                for (Table catalog_tbl : best_set) {
                    proc_hints.force_replication.add(catalog_tbl.getName());
                } // FOR
                continue;
            }

            //
            // If we're down here, then there is nothing else that needs to be
            // done!
            //
            break;
        } // WHILE
        return (ptree);
    }

    /**
     * @param ptree
     * @param parent
     */
    protected void buildPartitionTree(final PartitionTree ptree, final DesignerVertex parent, final AccessGraph agraph, final DesignerHints hints) throws Exception {
        boolean is_root = false;
        if (!ptree.containsVertex(parent)) {
            LOG.info("Starting heuristic PartitionTree generation at root '" + parent + "'");
            ptree.addVertex(parent);
            is_root = true;
        }
        if (!parent.hasAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name())) {
            parent.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.HASH);
        }

        // Replication: Check whether the hints tell us to force this table as
        // replicated
        if (hints.force_replication.contains(CatalogKey.createKey(parent.getCatalogItem()))) {
            LOG.info("Marking read-only " + parent + " as replicated");
            parent.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), null);
            parent.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.REPLICATION);
            assert (ptree.isReplicated(parent));
            return;
        }

        // --------------------------------------------------------
        // STEP #1: Get Candidate Children
        // --------------------------------------------------------

        // Starting at the current vertex, get all vertices at the next depth
        // level
        SortedSet<DesignerVertex> next = new TreeSet<DesignerVertex>(new VertexComparator(agraph, parent));
        int parent_depth = ptree.getDepth(parent);
        Table parent_table = parent.getCatalogItem();

        LOG.debug("===============================================");
        LOG.debug("Current Parent: " + parent + " " + parent.getAttributes(ptree));
        LOG.debug("Current Depth:  " + parent_depth);
        LOG.debug("Successors:     " + agraph.getSuccessors(parent));

        // Look through all the vertices that our parent is adjacent to in the
        // AccessGraph
        // and come up with a list of the next vertices to visit
        DesignerVertex parent_root = ptree.getRoot(parent);
        for (DesignerVertex vertex : agraph.getSuccessors(parent)) {
            boolean force = false;
            Table vertex_tbl = vertex.getCatalogItem();

            // Skip any self-references
            if (vertex == parent)
                continue;
            // Skip any vertices that are marked as replicated
            if (ptree.isReplicated(vertex)) {
                LOG.debug("Skipping " + vertex + " because it is already marked for replication");
                continue;
            }

            // Force dependencies
            if (hints.force_dependency.containsKey(CatalogKey.createKey(vertex_tbl))) {
                String force_dependency = hints.force_dependency.get(CatalogKey.createKey(vertex_tbl));
                // We'll force this We're allowed to break this forced
                // dependency if the current parent
                // is a descendant of the table that the child should be forced
                // to
                if (force_dependency != null) {
                    Table force_tbl = CatalogKey.getFromKey(info.catalogContext.database, force_dependency, Table.class);
                    if (parent_table.equals(force_tbl)) {
                        force = true;
                        LOG.debug("Forcing dependency: " + parent_table + "->" + vertex_tbl);
                    } else if (info.dependencies.getDescendants(force_tbl).contains(parent_table) && (ptree.containsVertex(vertex) || hints.force_replication.contains(force_dependency))) {
                        LOG.debug("Allowing take over of dependency: " + parent_table + "->" + vertex_tbl);
                    } else {
                        LOG.debug("Not allowing: " + parent_table + "->" + vertex_tbl);
                        LOG.debug(info.dependencies.getDescendants(force_tbl));
                        LOG.debug("ptree.containsVertex: " + ptree.containsVertex(vertex));
                        continue;
                    }
                }
            }

            // We then need to check whether the current parent table is a
            // descendant of
            // the other vertex in the DependencyGraph. This to check that you
            // don't violate
            // a foreign key dependency that may be several vertices removed
            List<DesignerEdge> path = info.dgraph.getPath(vertex, parent);
            if (!path.isEmpty()) {
                LOG.debug("Skipping " + vertex + " because it is an ancestor of " + parent + " in the DependencyGraph");
                continue;
            }

            // If this vertex is already in the PartitionTree, we need to check
            // whether our
            // current parent has an edge with a greater weight to the vertex
            // than the one it
            // currently has in the tree.
            //
            // What if the other vertex is a direct ascendant of the current
            // parent? That means
            // if we break the edge then the whole path will get messed up and
            // all the vertices
            // will be orphans again. Therefore, I think it should only move the
            // vertex if
            // the edge is greater *AND* it's not an ascendant of the current
            // parent.
            if (ptree.containsVertex(vertex)) {
                if (ptree.getPath(parent).contains(vertex)) {
                    LOG.debug("Skipping " + vertex + " because it is an ancestor of " + parent + " in the PartitionTree");
                    continue;
                }

                // Now look to whether there is a new Edge in the AccessGraph
                // with a greater
                // weight than the one that is currently being used in the
                // PartitionTree
                // We will only move the vertex if it's in the same tree
                DesignerVertex vertex_orig_parent = ptree.getParent(vertex);
                if (vertex_orig_parent != null) {
                    // Check whether these guys have the same root
                    // If they don't, then we won't move the child.
                    DesignerVertex child_root = ptree.getRoot(vertex);
                    if (!child_root.equals(parent_root)) {
                        LOG.debug("Skipping " + vertex + " because it's in a different partition tree (" + child_root + "<->" + parent_root + ")");
                        continue;
                    }

                    DesignerEdge orig_edge = null;
                    Double max_weight = null;
                    try {
                        orig_edge = ptree.findEdge(vertex_orig_parent, vertex);
                        // TODO: Need to think about whether it makes sense to
                        // take the total weight
                        // or whether we need to consider
                        max_weight = orig_edge.getTotalWeight();
                    } catch (Exception ex) {
                        LOG.error(vertex + " => " + vertex_orig_parent);
                        if (orig_edge != null) {
                            LOG.error(orig_edge.debug());
                        } else {
                            LOG.error("ORIG EDGE: null");
                        }
                        ex.printStackTrace();
                        System.exit(1);
                    }
                    DesignerEdge max_edge = orig_edge;
                    for (DesignerEdge candidate_edge : agraph.findEdgeSet(parent, vertex)) {
                        Double candidate_weight = (Double) candidate_edge.getAttribute(PartitionTree.EdgeAttributes.WEIGHT.name());
                        if (candidate_weight > max_weight) {
                            max_edge = candidate_edge;
                            max_weight = candidate_weight;
                        }
                    } // FOR
                      //
                      // If the edge has changed, then we need to add it to our
                      // list of next vertices to visit.
                      // What if there isn't an AttributeSet that matches up
                      // with the parent? Then
                      // we just broke up the graph for no good reason
                      //
                    if (!force && max_edge.equals(orig_edge)) {
                        LOG.debug("Skipping " + vertex + " because there was not an edge with a greater weight than what it currently has in the PartitionTree");
                        continue;
                    } else {
                        //
                        // Check whether this child was our parent before, then
                        // we need to make sure that
                        // we switch ourselves to the HASH partition method
                        // instead of MAP
                        //
                        if (ptree.getParent(parent) != null && ptree.getParent(parent).equals(vertex)) {
                            parent.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.HASH);
                        }
                        LOG.debug("Remove existing child " + vertex + " from PartitionTree");
                        ptree.removeChild(vertex);
                    }
                }
            }
            LOG.debug("Adding " + vertex + " to the list of the next nodes to visit");
            next.add(vertex);
        } // FOR

        // --------------------------------------------------------
        // STEP #2: Selecting Partitioning Attribute
        // --------------------------------------------------------

        if (!next.isEmpty()) {

            //
            // If we're the root, then we need to select our partitioning
            // attribute
            //
            if (is_root) {
                TablePartitionSets attributes = new TablePartitionSets((Table) parent.getCatalogItem());
                boolean debug = parent.getCatalogItem().getName().equals("STOCK");
                for (DesignerVertex child : next) {
                    //
                    // We now need to pick what edge from the AccessGraph to use
                    // as the dependency edge
                    // in our partition mapping
                    //
                    Table catalog_child_tbl = child.getCatalogItem();
                    LOG.debug("Looking for edge between " + parent + " and " + child + ": " + agraph.findEdgeSet(parent, child));
                    for (DesignerEdge edge : agraph.findEdgeSet(parent, child)) {
                        LOG.debug("Creating AttributeSet entry for " + edge);
                        //
                        // We only want to use edges that are used in joins.
                        //
                        AccessGraph.AccessType type = (AccessGraph.AccessType) edge.getAttribute(AccessGraph.EdgeAttributes.ACCESSTYPE.name());
                        if (!AccessGraph.AccessType.JOINS.contains(type))
                            continue;
                        attributes.add(catalog_child_tbl, edge);
                    } // FOR
                } // FOR
                if (debug) {
                    LOG.debug(attributes.debug());
                    LOG.debug("---------------------");
                }
                if (attributes.size() > 1)
                    attributes.generateSubSets();
                if (debug) {
                    LOG.debug(attributes.debug());
                    LOG.debug("---------------------");
                }

                //
                // Now get the list of AttributeSets that have the highest
                // weights
                //
                Set<TablePartitionSets.Entry> asets = attributes.getMaxWeightAttributes();
                if (debug) {
                    System.out.println(asets);
                }

                TablePartitionSets.Entry aset = null;
                if (asets.isEmpty()) {
                    LOG.debug("Skipping vertex " + parent + " because no attributes to its children were found");
                    return; // throw new
                            // Exception("ERROR: Failed to generate AttributeSets for parent '"
                            // + parent + "'");
                } else if (asets.size() > 1) {
                    //
                    // XXX: Pick the attribute with the longest path to a root
                    // in the dependency graph
                    //
                    TablePartitionSets.Entry best_entry = null;
                    int best_length = Integer.MIN_VALUE;
                    for (TablePartitionSets.Entry entry : asets) {
                        for (Column catalog_col : entry) {
                            List<Column> ancestors = info.dependencies.getAncestors(catalog_col);
                            int length = ancestors.size();
                            LOG.debug(catalog_col + " ==> " + ancestors + " [length=" + length + "]");
                            if (length > best_length) {
                                best_entry = entry;
                                best_length = length;
                            }
                        } // FOR
                    } // FOR
                    if (best_entry == null) {
                        LOG.fatal("Unable to handle more than one AttributeSet for parent '" + parent + "' [" + asets.size() + "]");
                        System.exit(1);
                    }
                    LOG.debug("Choose PartitionSet.Entry " + best_entry + " because it has a path length of " + best_length);
                    aset = best_entry;
                } else {
                    aset = CollectionUtil.first(asets);
                }

                //
                // We need to figure out which attribute to select if there are
                // multiple ones
                // Well, one way is to pick one that
                parent.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), CollectionUtil.first(aset));
            } // is_root

            //
            // This AttributeSet determines how we want to partition the parent
            // node.
            // Therefore, we can only attach those children that have edges that
            // use these attributes
            //
            List<DesignerVertex> next_to_visit = new ArrayList<DesignerVertex>();
            Column parent_attribute = (Column) parent.getAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name());
            for (DesignerVertex child : next) {
                for (DesignerEdge edge : agraph.findEdgeSet(parent, child)) {
                    //
                    // Find the edge that links parent to this child
                    // If no edge exists, then the child can't be linked to the
                    // parent
                    //
                    PredicatePairs cset = (PredicatePairs) edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
                    Collection<Column> entries = cset.findAllForOther(Column.class, parent_attribute);
                    if (!entries.isEmpty()) {
                        next_to_visit.add(child);
                        assert (entries.size() == 1) : "Multiple entries from " + parent + " to " + child + ": " + entries;
                        try {
                            if (!ptree.containsVertex(parent))
                                ptree.addVertex(parent);

                            DesignerEdge new_edge = ptree.createEdge(parent, child, edge);
                            LOG.debug("Creating new edge " + new_edge + " in PartitionTree");
                            LOG.debug(new_edge.debug(ptree));

                            child.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), CollectionUtil.first(entries));
                            child.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.MAP);

                            //
                            // For now we can only link ourselves to the
                            // parent's attribute
                            //
                            child.setAttribute(ptree, PartitionTree.VertexAttributes.PARENT_ATTRIBUTE.name(), parent.getAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name()));

                            // if
                            // (parent.getTable().getName().equals("DISTRICT"))
                            // {
                            // System.out.println(parent.getTable().getName() +
                            // "." + parent_attribute.getName() + " -> " +
                            // child.getTable().getName() + "." +
                            // CollectionUtil.getFirst(entries).getName());
                            // System.exit(1);
                            // }
                        } catch (Exception ex) {
                            LOG.fatal(ex.getMessage());
                            ex.printStackTrace();
                            System.exit(1);
                        }
                        break;
                    }
                } // FOR
            } // FOR
            for (DesignerVertex child : next_to_visit) {
                this.buildPartitionTree(ptree, child, agraph, hints);
            }
            //
            // If the current parent doesn't have any children, then we
            // we need to decide how to partition it based on the
            // self-referencing edges
            //
        } else if (is_root || !ptree.containsVertex(parent)) {
            LOG.debug("Parent " + parent + " does not have any children");
            DesignerEdge partition_edge = null;
            double max_weight = 0;
            for (DesignerEdge edge : agraph.getIncidentEdges(parent)) {
                AccessGraph.AccessType type = (AccessGraph.AccessType) edge.getAttribute(AccessGraph.EdgeAttributes.ACCESSTYPE.name());
                if (type != AccessGraph.AccessType.SCAN)
                    continue;
                Double weight = 0.0d; // FIXME
                                      // (Double)edge.getAttribute(AccessGraph.EdgeAttributes.WEIGHT.name());
                if (weight > max_weight) {
                    partition_edge = edge;
                    max_weight = weight;
                }
            } // FOR
            if (partition_edge != null) {
                PredicatePairs cset = (PredicatePairs) partition_edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
                Collection<Column> attributes = cset.findAllForParent(Column.class, parent_table);
                parent.setAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name(), CollectionUtil.first(attributes));
                parent.setAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name(), PartitionMethodType.HASH);
                LOG.debug(parent + parent.debug(ptree));
            } else {
                LOG.debug("Removing " + parent + " from PartitionTree because it does not contain an edge");
                ptree.removeVertex(parent);
            }
        }
        return;
    }

    /**
     * @param ptrees
     * @return
     */
    protected PartitionPlan generatePlan(List<PartitionTree> ptrees) {
        //
        // Iterate through all the ptrees and generate a PartitionMapping table
        //
        Map<PartitionTree, PartitionPlan> partition_plans = new HashMap<PartitionTree, PartitionPlan>();
        for (final PartitionTree ptree : ptrees) {
            final PartitionPlan partition_plan = new PartitionPlan();
            assert (ptree != null);

            //
            // A PartitionTree may have multiple roots because some vertices
            // could be isolated
            // for replication or failed to be important enough to be linked in
            // the "main" tree
            //
            for (DesignerVertex root : ptree.getRoots()) {
                // System.out.println("ROOT: " + root);
                new VertexTreeWalker<DesignerVertex, DesignerEdge>(ptree) {
                    @Override
                    protected void populate_children(VertexTreeWalker.Children<DesignerVertex> children, DesignerVertex element) {
                        for (DesignerVertex v : this.getGraph().getSuccessors(element)) {
                            if (!this.hasVisited(v)) {
                                children.addAfter(v);
                            }
                        } // FOR
                        return;
                    }

                    @Override
                    protected void callback(DesignerVertex element) {
                        LOG.debug("SimpleCountingMapper.CALLBACK -> " + element.getCatalogItem());
                        DesignerVertex parent = this.getPrevious();

                        PartitionMethodType method = (PartitionMethodType) (element.getAttribute(ptree, PartitionTree.VertexAttributes.METHOD.name()));
                        Column attribute = null;
                        Table parent_table = null;
                        Column parent_attribute = null;

                        if (method == PartitionMethodType.REPLICATION) {
                            // Anything???
                        } else {
                            attribute = (Column) element.getAttribute(ptree, PartitionTree.VertexAttributes.ATTRIBUTE.name());
                            //
                            // If this vertex is a dependent on a parent, then
                            // we also need to get the
                            // mapping of columns
                            //
                            if (parent != null) {
                                DesignerEdge edge = ptree.findEdge(parent, element);
                                if (edge == null) {
                                    LOG.fatal("Failed to find edge between parent '" + parent + "' and child '" + element + "'");
                                    this.stop();
                                    return;
                                }
                                parent_attribute = (Column) element.getAttribute(ptree, PartitionTree.VertexAttributes.PARENT_ATTRIBUTE.name());
                                parent_table = parent.getCatalogItem();
                                method = PartitionMethodType.MAP; // Why do we
                                                                  // have to set
                                                                  // this here?
                                // if (parent_attribute == null) {
                                // PartitionEntry entry = new
                                // PartitionEntry(element.getTable(), method,
                                // attribute, parent_table, parent_attribute);
                                // System.out.println(element.getAttributeValues(ptree));
                                // System.out.println(entry.toString());
                                // System.exit(1);
                                // }
                            }
                        }

                        TableEntry entry = new TableEntry(method, attribute, parent_table, parent_attribute);
                        partition_plan.getTableEntries().put((Table) element.getCatalogItem(), entry);
                        return;
                    }
                }.traverse(root);
            } // FOR roots
            LOG.info(partition_plan);
            partition_plans.put(ptree, partition_plan);
        } // FOR trees

        //
        // Now for each relation, make a tally for the different ways that it
        // could be partitioned
        // This will then be used to generate the final PartitionMapping
        //
        PartitionPlan pplan = new PartitionPlan();
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            //
            // For each table, look at the PartitionPlan entries that we created
            // above and see
            // whether it references our table. If it does, then we need to
            // increase our count
            // by one.
            //
            Map<TableEntry, Double> counts = new HashMap<TableEntry, Double>();
            LOG.debug("Counting PartitionPlan entries for " + catalog_tbl);
            for (PartitionTree ptree : partition_plans.keySet()) {
                PartitionPlan partition_plan = partition_plans.get(ptree);
                // System.out.println("Mapping Tables: " + mapping.keySet());
                //
                // We found a partition plan that references our table, so then
                // we need to grab
                // the entry and include it in our count list. Note that the
                // PartitionPlan.Entry
                // object knows how to properly tell whether it has the same
                // attributes as
                // other entry objects, so the count should be properly updated.
                //
                if (partition_plan.getTableEntries().containsKey(catalog_tbl)) {
                    //
                    // Exclude HASH entries without attributes...
                    //
                    TableEntry entry = partition_plan.getTableEntries().get(catalog_tbl);
                    if (entry.getMethod() == PartitionMethodType.HASH && entry.getAttribute() == null) {
                        LOG.warn("Skipping entry for " + catalog_tbl + " because it does not have any partitioning attributes");
                    } else {
                        LOG.debug("Match: " + partition_plan);
                        //
                        // We need to weight this entry by the weight of the
                        // PartitionTree
                        // that it was derived from
                        //
                        double count = ptree.getWeight();
                        if (counts.containsKey(entry))
                            count += counts.get(entry);
                        counts.put(entry, count);
                    }
                } // FOR
            } // FOR

            //
            // If a table was either hashed or mapped on the same attributes,
            // then
            // always go for map
            //
            Iterator<TableEntry> it = counts.keySet().iterator();
            while (it.hasNext()) {
                TableEntry entry0 = it.next();
                if (entry0.getMethod() == PartitionMethodType.MAP)
                    continue;

                boolean remove = false;
                for (TableEntry entry1 : counts.keySet()) {
                    if (entry0 == entry1)
                        continue;
                    if (entry0.getMethod() == PartitionMethodType.HASH && entry1.getMethod() == PartitionMethodType.MAP && entry0.getAttribute().equals(entry1.getAttribute())) {
                        remove = true;
                    }
                } // FOR
                if (remove) {
                    LOG.info("Removing " + entry0 + " because a duplicate entry for a MAP already exists");
                    it.remove();
                    counts.remove(entry0);
                }
            } // WHILE

            //
            // If our counts for the current table is not empty, then we need to
            // need to pick
            // the one with the greatest count
            //
            if (!counts.isEmpty()) {
                for (TableEntry entry : counts.keySet()) {
                    LOG.debug("[" + counts.get(entry) + "]: " + entry);
                } // FOR
                  //
                  // Loop through and pick the entries with the greatest weight
                  // We use a set to warn about multiple entries that could be
                  // picked (which is another decision problem)
                  //
                Set<TableEntry> picked_entries = new HashSet<TableEntry>();
                double max_cnt = 0;
                for (TableEntry entry : counts.keySet()) {
                    double entry_cnt = counts.get(entry);
                    //
                    // If the entry's weight is the same or equal to the current
                    // max weight, then
                    // add it to our list of possible selections. Note that if
                    // it's greater than the
                    // current max weight, then we need to clear our previous
                    // entries
                    //
                    if (entry_cnt >= max_cnt) {
                        if (entry_cnt > max_cnt)
                            picked_entries.clear();
                        picked_entries.add(entry);
                        max_cnt = entry_cnt;
                    }
                } // FOR
                assert (picked_entries.isEmpty() == false);
                if (picked_entries.size() > 1) {
                    LOG.warn("Multiple entries found with the same count for " + catalog_tbl + ". Picking the first one that has a parent");
                    pplan.getTableEntries().put(catalog_tbl, CollectionUtil.first(picked_entries));
                } else {
                    // Just grab the only one and stick it in the PartitionPlan
                    pplan.getTableEntries().put(catalog_tbl, CollectionUtil.first(picked_entries));
                }
                // System.out.println(catalog_tbl + " => " +
                // final_mapping.get(catalog_tbl).toString());
                //
                // This is bad news all around...
                //
            } else {
                LOG.warn("Failed to find any PartitionPlan entries that reference " + catalog_tbl);
            }
        } // FOR tables

        //
        // HACK: Add in any tables we missed as replicated
        //
        for (Table catalog_tbl : info.catalogContext.database.getTables()) {
            if (pplan.getTableEntries().get(catalog_tbl) == null) {
                pplan.getTableEntries().put(catalog_tbl, new TableEntry(PartitionMethodType.REPLICATION, null, null, null));
            }
        } // FOR

        pplan.initializeDependencies();
        return (pplan);
    }

    /**
     * @param graph
     * @param agraph
     * @return
     * @throws Exception
     */
    protected List<DesignerVertex> createCandidateRoots(final DesignerHints hints, final IGraph<DesignerVertex, DesignerEdge> agraph) throws Exception {
        LOG.debug("Searching for candidate roots...");
        if (agraph == null)
            throw new NullPointerException("AccessGraph is Null");

        // For each vertex, we are going come up with a weight that determines
        // the order
        // in which we will try to partition their descendant tables. We want to
        // sort the vertices in descending order by their weight.
        final Map<DesignerVertex, Double> root_weights = new HashMap<DesignerVertex, Double>();
        Comparator<DesignerVertex> root_comparator = new Comparator<DesignerVertex>() {
            @Override
            public int compare(DesignerVertex v0, DesignerVertex v1) {
                Double w0 = root_weights.get(v0);
                Double w1 = root_weights.get(v1);

                if (w0.equals(w1)) {
                    return (v1.getCatalogItem().compareTo(v0.getCatalogItem()));
                }
                return (w1.compareTo(w0));
            }
        };

        // Loop through all of the vertices in our DependencyGraph and calculate
        // the weight of the edges from the candidate root to their descendants
        final List<DesignerVertex> roots = new ArrayList<DesignerVertex>();
        final TreeSet<DesignerVertex> candidates = new TreeSet<DesignerVertex>(root_comparator);
        for (final DesignerVertex vertex : info.dgraph.getVertices()) {
            // Skip if this table isn't even used in this procedure
            if (!agraph.containsVertex(vertex))
                continue;
            // Also skip if this table is marked for replication
            if (hints.force_replication.contains(vertex.getCatalogItem().getName()))
                continue;

            // We only can only use this vertex as a candidate root if
            // none of its parents (if it even has any) are used in the
            // AccessGraph or are
            // not marked for replication
            boolean valid = true;
            Collection<DesignerVertex> parents = info.dgraph.getPredecessors(vertex);
            for (DesignerVertex other : agraph.getNeighbors(vertex)) {
                if (parents.contains(other) && !hints.force_replication.contains(other.getCatalogItem().getName())) {
                    LOG.debug("SKIP " + vertex + " [" + other + "]");
                    valid = false;
                    break;
                }
            } // FOR
            if (!valid)
                continue;
            LOG.debug("CANDIDATE: " + vertex);
            root_weights.put(vertex, 0d);

            // We now need to set the weight of the candidate.
            // The first way I did this was to count the number of outgoing
            // edges from the candidate
            // Now I'm going to use the weights of the outgoing edges in the
            // AccessGraph.
            new VertexTreeWalker<DesignerVertex, DesignerEdge>(info.dgraph) {
                @Override
                protected void callback(DesignerVertex element) {
                    // Get the total weights from this vertex to all of its
                    // descendants
                    if (agraph.containsVertex(element)) {
                        double total_weight = root_weights.get(vertex);
                        Collection<DesignerVertex> descedents = info.dgraph.getDescendants(element);
                        // LOG.debug(" -> " + element + ": " + descedents);
                        for (DesignerVertex descendent : descedents) {
                            if (descendent != element && agraph.containsVertex(descendent)) {
                                // QUESTION:
                                // Do we care whether edges are all reference
                                // the same attributes?
                                // How would this work in TPC-E, since there a
                                // bunch of root tables
                                // that are referenced but do not have long
                                // paths along the same
                                // foreign key dependencies that we have in
                                // TPC-C (e.g., W_ID)
                                for (DesignerEdge edge : agraph.findEdgeSet(element, descendent)) {
                                    // XXX: 2010-05-07
                                    // We multiply the edge weights by the
                                    // distance of the destination vertex
                                    // to the root of the candidate root. This
                                    // means that we will weight
                                    // long paths more important than short
                                    // ones, because that means we will
                                    // get to include more related partitions
                                    // together
                                    LOG.debug(edge + " [" + this.getDepth() + "]: " + edge.getTotalWeight());
                                    total_weight += (edge.getTotalWeight() * (this.getDepth() + 1));
                                } // FOR
                            }
                        } // FOR
                        root_weights.put(vertex, total_weight);
                    }
                }
            }.traverse(vertex);
            candidates.add(vertex);
        } // FOR

        StringBuilder buffer = new StringBuilder();
        buffer.append("Found ").append(candidates.size()).append(" candidate roots and ranked them as follows:\n");
        int ctr = 0;
        for (DesignerVertex vertex : candidates) {
            buffer.append("\t[").append(ctr++).append("] ").append(vertex).append("  Weight=").append(root_weights.get(vertex)).append("\n");
            roots.add(vertex);
        } // FOR
        LOG.debug(buffer.toString());
        // LOG.info(buffer.toString());

        return (roots);
    }
}