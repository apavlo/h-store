package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;

import edu.brown.designer.AccessGraph;
import edu.brown.designer.DesignerEdge;
import edu.brown.designer.DesignerEdge.Members;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.PredicatePairs;

public class TablePartitionSets extends HashSet<TablePartitionSets.Entry> {
    private static final long serialVersionUID = 3462969885348478117L;
    private static final Logger LOG = Logger.getLogger(TablePartitionSets.class.getName());

    private final Map<Table, Set<Entry>> table_entry_xref = new HashMap<Table, Set<Entry>>();
    private final Map<PredicatePairs, Entry> cset_entry_xref = new HashMap<PredicatePairs, Entry>();
    private final Table parent_table;

    /**
     * A unique set of attributes that the parent table can be partitioned on
     */
    public class Entry extends HashSet<Column> {
        private static final long serialVersionUID = 8592594865097644058L;

        protected double weight = 0;

        private final Map<Table, Set<PredicatePairs>> table_cset_xref = new HashMap<Table, Set<PredicatePairs>>();
        private final Map<Table, Set<DesignerEdge>> table_edge_xref = new HashMap<Table, Set<DesignerEdge>>();
        private final Map<Table, Collection<Column>> table_partition_xref = new HashMap<Table, Collection<Column>>();

        /**
         * @param other
         */
        public void merge(Entry other) {
            //
            // Find the intersecting entries in order to scrub the ones that
            // don't belong from the ColumnSets of the merging into
            //
            for (CatalogType catalog_item : other) {
                if (!this.contains(catalog_item)) {
                    //
                    // Loop through all the ColumnSets for the tables that
                    // reference this entry
                    // and remove the attribute from them
                    //
                    for (Table catalog_tbl : other.table_cset_xref.keySet()) {
                        for (PredicatePairs cset : other.table_cset_xref.get(catalog_tbl)) {
                            cset.removeAll(cset.findAll(catalog_item));
                        } // FOR
                    } // FOR
                }
            } // FOR

            this.table_cset_xref.putAll(other.table_cset_xref);
            this.table_edge_xref.putAll(other.table_edge_xref);
            this.table_partition_xref.putAll(other.table_partition_xref);
            return;
        }

        /**
         * Return all the tables used in this PartitionSet
         * 
         * @return
         */
        public Set<Table> getTables() {
            return (this.table_cset_xref.keySet());
        }

        /**
         * Return the parent table of this Entry object
         * 
         * @return
         */
        public Table getParent() {
            return (TablePartitionSets.this.parent_table);
        }

        /**
         * Returns the edge that connects the parent table in the PartitionSets
         * object to the child table provided
         * 
         * @param child_table
         * @return
         */
        public DesignerEdge getEdge(Table child_table) {
            DesignerEdge ret = null;
            if (this.table_edge_xref.containsKey(child_table)) {
                ret = CollectionUtil.first(this.table_edge_xref.get(child_table));
            }
            return (ret);
        }

        /**
         * Returns the list of columns that were used to link the child table
         * provided to the parent table based on this AttributeSet
         * 
         * @param child_table
         * @return
         */
        public Collection<Column> getChildAttributes(Table child_table) {
            return (this.table_partition_xref.get(child_table));
        }

        /**
         * @return
         */
        public double getWeight() {
            return this.weight;
        }

        /**
         * @param catalog_tbl
         * @return
         */
        public Set<PredicatePairs> getColumnSets(Table catalog_tbl) {
            return (this.table_cset_xref.get(catalog_tbl));
        }

        public String debug() {
            String ret = this.getClass().getSimpleName() + "\n";
            ret += "  Attributes: " + this.toString() + "\n";
            ret += "  Weight:     " + this.getWeight() + "\n";
            ret += "  Tables:     " + this.table_edge_xref.keySet();
            return (ret);
        }
    } // END CLASS

    public TablePartitionSets(Table parent_table) {
        super();
        this.parent_table = parent_table;
    }

    public Table getParentTable() {
        return this.parent_table;
    }

    public boolean add(Table child_table, DesignerEdge edge) {
        //
        // We first need to convert the ColumnSet into a set of attributes
        // that "connect" the child table with the parent
        //
        PredicatePairs cset = null;
        Entry entry = new Entry();
        if (this.parent_table == child_table) {
            cset = (PredicatePairs) edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name());
            entry.addAll(cset.findAllForParent(Column.class, child_table));
        } else {
            cset = ((PredicatePairs) edge.getAttribute(AccessGraph.EdgeAttributes.COLUMNSET.name())).createPredicatePairsForParent(Column.class, child_table);
            entry.addAll(cset.findAllForOtherParent(Column.class, child_table));
        }
        double weight = (Double) edge.getAttribute(Members.WEIGHTS.name());
        entry.weight = weight;
        // System.out.println(cset + " Weight=" + entry.weight);

        //
        // Check whether we already contain this AttributeSet
        // If we do, then we need to add the weight of the one we're being given
        // to the one that we already have in there
        //
        boolean found = false;
        for (Entry aset : this) {
            if (aset.equals(entry)) {
                found = true;
                aset.weight += weight;
                entry = aset;
                break;
            }
        } // FOR
        if (!found)
            super.add(entry);

        //
        // Build our various index structures. We probably should probably think
        // this through
        // a bit more than just making anything we want here...
        //

        //
        // Table -> ColumnSets
        //
        if (!entry.table_cset_xref.containsKey(child_table)) {
            entry.table_cset_xref.put(child_table, new HashSet<PredicatePairs>());
        }
        entry.table_cset_xref.get(child_table).add(cset);

        //
        // Table -> Entry
        //
        if (!this.table_entry_xref.containsKey(child_table)) {
            this.table_entry_xref.put(child_table, new HashSet<Entry>());
        }
        this.table_entry_xref.get(child_table).add(entry);

        //
        // ColumnSet -> AttributeSet
        //
        this.cset_entry_xref.put(cset, entry);

        //
        // Child Table -> Edges
        //
        if (!entry.table_edge_xref.containsKey(child_table)) {
            entry.table_edge_xref.put(child_table, new HashSet<DesignerEdge>());
        }
        entry.table_edge_xref.get(child_table).add(edge);

        //
        // Child Table -> Attributes
        //
        if (entry.table_partition_xref.containsKey(child_table)) {
            LOG.fatal("The AttributeSet for parent table '" + this.parent_table + "' " + "already contains attributes for child table '" + child_table + "'");
            return (false);
        }
        entry.table_partition_xref.put(child_table, cset.findAllForOtherParent(Column.class, this.parent_table));

        return (true);
    }

    public Set<Entry> getMaxWeightAttributes() {
        Set<Entry> ret = new HashSet<Entry>();
        double max_weight = 0;
        for (Entry aset : this) {
            if (aset.weight == max_weight) {
                ret.add(aset);
            } else if (aset.weight > max_weight) {
                ret.clear();
                ret.add(aset);
                max_weight = aset.weight;
            }
        } // FOR
        return (ret);
    }

    /**
     * @throws Exception
     */
    public void generateSubSets() throws Exception {
        //
        // Simple hack for now
        //
        LOG.info("Trying to generate entry subsets");
        System.out.println(this.debug());
        Set<Entry> remove = new HashSet<Entry>();
        for (Entry entry0 : this) {
            for (Entry entry1 : this) {
                if (entry0 == entry1)
                    continue;
                if (entry0.containsAll(entry1) && entry0.size() > entry1.size()) {
                    LOG.info("Merging " + entry0 + " into " + entry1);
                    entry1.merge(entry0);
                    remove.add(entry0);
                    break;
                } else {
                    LOG.debug("entry0: [" + entry0.size() + "] " + entry0);
                    LOG.debug("entry1: [" + entry1.size() + "] " + entry1);
                    LOG.debug("entry0.containsAll(entry1): " + entry0.containsAll(entry1));
                    LOG.debug("entry1.containsAll(entry0): " + entry1.containsAll(entry0));
                }
            } // FOR
            if (!remove.isEmpty())
                break;
        } // FOR
        if (!remove.isEmpty()) {
            this.removeAll(remove);
            LOG.debug(this.debug());
        } else {
            LOG.warn("Failed to find any entries to merge!");
            // if (this.parent_table.getName().equals("CUSTOMER"))
            // System.exit(1);
        }

        return;
    }

    public String debug() {
        String ret = this.getClass().getSimpleName() + ": " + this.parent_table + "\n";
        for (Entry entry : this) {
            ret += entry.debug() + "\n";
        }
        return (ret);
    }

}
