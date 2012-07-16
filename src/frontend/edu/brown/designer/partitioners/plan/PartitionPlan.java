package edu.brown.designer.partitioners.plan;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONStringer;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.PartitionMethodType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.NullProcParameter;
import edu.brown.catalog.special.RandomProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.catalog.special.VerticalPartitionColumn;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.partitioners.PartitionerUtil;
import edu.brown.designer.partitioners.VerticalPartitionerUtil;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.AbstractTreeWalker;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.JSONSerializable;
import edu.brown.utils.JSONUtil;

/**
 * @author pavlo
 */
public class PartitionPlan implements JSONSerializable {
    protected static final Logger LOG = Logger.getLogger(PartitionPlan.class.getName());

    public enum Members {
        TABLE_ENTRIES, PROC_ENTRIES,
    }

    /**
     * Table Partition Entries
     */
    public final Map<Table, TableEntry> table_entries = new TreeMap<Table, TableEntry>();

    /**
     * Procedure Partition Entries
     */
    public final Map<Procedure, ProcedureEntry> proc_entries = new TreeMap<Procedure, ProcedureEntry>();

    /**
     * For each table, we store an ordered list tables up to the root of the
     * schema tree Table -> Vector
     * <Table>
     */
    private transient final Map<Table, List<Table>> table_ancestors = new HashMap<Table, List<Table>>();
    private transient Map<Table, Table> table_roots = new HashMap<Table, Table>();

    /**
     * List of children for each table
     */
    private transient final Map<Table, Set<Table>> table_children = new HashMap<Table, Set<Table>>();

    /**
     * For each table we store a set of the tables that are dependent to it
     * Table -> Set
     * <Table>
     */
    private transient final Map<Table, Set<Table>> table_descendants = new HashMap<Table, Set<Table>>();

    /**
     * Constructor
     */
    public PartitionPlan() {
        super();
    }

    /**
     * Copy Constructor
     * 
     * @param pplan
     */
    public PartitionPlan(PartitionPlan pplan) {
        this();
        createFromMap(this, pplan.toMap());
    }

    public void addTablePartitionEntry(Table catalog_tbl, TableEntry pentry) {
        this.table_entries.put(catalog_tbl, pentry);
    }

    public void addProcedurePartitionEntry(Procedure catalog_proc, ProcedureEntry pentry) {
        this.proc_entries.put(catalog_proc, pentry);
    }

    public Map<Table, TableEntry> getTableEntries() {
        return (this.table_entries);
    }

    public TableEntry getTableEntry(Table catalog_tbl) {
        assert (catalog_tbl != null);
        return (this.table_entries.get(catalog_tbl));
    }

    public Map<Procedure, ProcedureEntry> getProcedureEntries() {
        return (this.proc_entries);
    }

    public ProcedureEntry getProcedureEntry(Procedure catalog_proc) {
        assert (catalog_proc != null);
        return (this.proc_entries.get(catalog_proc));
    }

    public void setNullProcParameter(Procedure catalog_proc) {
        ProcedureEntry entry = new ProcedureEntry(PartitionMethodType.NONE);
        this.proc_entries.put(catalog_proc, entry);
    }

    public void setRandomProcParameter(Procedure catalog_proc) {
        ProcedureEntry entry = new ProcedureEntry(PartitionMethodType.RANDOM);
        this.proc_entries.put(catalog_proc, entry);
    }

    public int getTableCount() {
        return (this.table_entries.size());
    }

    public int getProcedureCount() {
        return (this.proc_entries.size());
    }

    /**
     * 
     */
    public void initializeDependencies() {
        for (Table catalog_tbl : this.table_entries.keySet()) {
            TableEntry entry = this.table_entries.get(catalog_tbl);
            this.table_descendants.put(catalog_tbl, new HashSet<Table>());
            this.table_children.put(catalog_tbl, new HashSet<Table>());

            // Children
            for (Table other_tbl : this.table_entries.keySet()) {
                if (catalog_tbl.equals(other_tbl))
                    continue;
                TableEntry other = this.table_entries.get(other_tbl);
                if (!entry.equals(other) && other.getParent() != null && other.getParent().equals(catalog_tbl)) {
                    this.table_children.get(catalog_tbl).add(other_tbl);
                }
            } // FOR

            // Ancestors
            final Vector<Table> ancestors = new Vector<Table>();
            new AbstractTreeWalker<Table>() {
                protected void populate_children(AbstractTreeWalker.Children<Table> children, Table element) {
                    TableEntry current_entry = table_entries.get(element);
                    Table parent = current_entry.getParent();
                    if (parent != null) {
                        TableEntry parent_entry = PartitionPlan.this.table_entries.get(parent);
                        if (parent_entry == null) {
                            LOG.warn(String.format("Missing parent entry %s for child %s: %s", parent, element, table_entries.keySet()));
                        } else {
                            // assert(parent_entry != null) :
                            // "Missing parent entry " + parent + " for " +
                            // element;
                            if (!this.hasVisited(parent))
                                children.addAfter(parent);
                        }
                    }
                };

                @Override
                protected void callback(Table element) {
                    if (element != this.getFirst()) {
                        ancestors.add(element);
                    }
                }
            }.traverse(catalog_tbl);
            this.table_ancestors.put(catalog_tbl, ancestors);
        } // FOR

        // Descendants & Roots
        // Table last_tbl = null;
        for (Table catalog_tbl : this.table_ancestors.keySet()) {
            for (Table ancestor_tbl : this.table_ancestors.get(catalog_tbl)) {
                if (!this.table_descendants.containsKey(ancestor_tbl)) {
                    this.table_descendants.put(ancestor_tbl, new HashSet<Table>());
                }
                this.table_descendants.get(ancestor_tbl).add(catalog_tbl);
            } // FOR

            Table root = (this.table_ancestors.get(catalog_tbl).isEmpty() ? catalog_tbl : CollectionUtil.last(this.table_ancestors.get(catalog_tbl)));
            this.table_roots.put(catalog_tbl, root);

            // last_tbl = catalog_tbl;
        } // FOR
    }

    /**
     * Apply the partitioning plan to the given catalog
     * 
     * @param catalog_db
     */
    public void apply(Database catalog_db) {
        this.apply(catalog_db, true);
    }

    public void apply(Database catalog_db, boolean enableVerticalPartitions) {
        final boolean debug = LOG.isDebugEnabled();
        if (debug)
            LOG.debug("Applying PartitionPlan to catalog");

        Set<VerticalPartitionColumn> vp_cols = new HashSet<VerticalPartitionColumn>();
        Collection<Table> tables = new HashSet<Table>(catalog_db.getTables());
        for (Table catalog_tbl : tables) {
            if (catalog_tbl.getSystable())
                continue;
            TableEntry pentry = this.table_entries.get(catalog_tbl);
            if (pentry != null) {
                if (debug)
                    LOG.debug("Applying PartitionEntry to " + catalog_tbl.getName() + ": " + pentry);

                if (pentry.getMethod() == PartitionMethodType.REPLICATION) {
                    catalog_tbl.setIsreplicated(true);
                    catalog_tbl.setPartitioncolumn(ReplicatedColumn.get(catalog_tbl));
                } else {
                    Column catalog_col = (Column) pentry.getAttribute();
                    assert (catalog_col != null);
                    if (catalog_col instanceof VerticalPartitionColumn && enableVerticalPartitions == false) {
                        catalog_col = ((VerticalPartitionColumn) catalog_col).getHorizontalColumn();
                    }
                    catalog_tbl.setPartitioncolumn(catalog_col);
                    catalog_tbl.setIsreplicated(false);
                }
                if (catalog_tbl.getPartitioncolumn() instanceof VerticalPartitionColumn) {
                    assert (enableVerticalPartitions) : "Unexpected " + catalog_tbl.getPartitioncolumn().fullName();
                    VerticalPartitionColumn vp_col = (VerticalPartitionColumn) catalog_tbl.getPartitioncolumn();
                    vp_cols.add(vp_col);
                }
            } else {
                if (debug)
                    LOG.warn("Missing PartitionEntry for " + catalog_tbl);
            }
        } // FOR

        // Create optimized queries
        if (vp_cols.isEmpty() == false) {
            boolean clearCache = false;
            for (VerticalPartitionColumn vc : vp_cols) {
                boolean ret = VerticalPartitionerUtil.generateOptimizedQueries(catalog_db, vc);
                if (ret == false) {
                    LOG.warn(String.format("Failed to generate optimized queries for %s\n%s", vc.getClass().getSimpleName(), vc));
                } else {
                    assert (vc.hasOptimizedQueries()) : vc;
                    Statement catalog_stmt = CollectionUtil.first(vc.getOptimizedQueries());
                    assert (catalog_stmt != null);
                    Procedure catalog_proc = catalog_stmt.getParent();
                    String stmtName = catalog_stmt.getName();
                    if (vc.isUpdateApplied() == false)
                        vc.applyUpdate();

                    catalog_stmt = catalog_proc.getStatements().get(stmtName);
                    AbstractPlanNode root = PlanNodeUtil.getRootPlanNodeForStatement(catalog_stmt, false);
                    if (debug)
                        LOG.debug(catalog_stmt.fullName() + "\n" + PlanNodeUtil.debug(root));
                    clearCache = true;
                }
            } // FOR
            if (clearCache) {
                CatalogUtil.clearCache(catalog_db);
                PlanNodeUtil.clearCache();
            }
        }

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            ProcedureEntry pentry = this.proc_entries.get(catalog_proc);
            if (catalog_proc.getSystemproc() || pentry == null || catalog_proc.getParameters().size() == 0)
                continue;
            if (debug)
                LOG.debug("Applying PartitionEntry to " + catalog_proc.getName() + ": " + pentry);

            ProcParameter catalog_proc_param = null;
            switch (pentry.getMethod()) {
                case NONE:
                    catalog_proc_param = NullProcParameter.singleton(catalog_proc);
                    break;
                case RANDOM:
                    catalog_proc_param = RandomProcParameter.singleton(catalog_proc);
                    break;
                case HASH:
                    catalog_proc_param = pentry.getAttribute();
                    break;
                default:
                    assert (false) : "Unexpected PartitionMethodType for " + catalog_proc + ": " + pentry.getMethod();
            } // SWITCH

            assert (catalog_proc_param != null) : "Null ProcParameter for " + catalog_proc;
            catalog_proc.setPartitionparameter(catalog_proc_param.getIndex());

            Boolean single_partition = pentry.isSinglePartition();
            if (single_partition != null)
                catalog_proc.setSinglepartition(single_partition);
        } // FOR
    }

    /**
     * Get the set of catalog items that differ between this PartitionPlan and
     * another PartitionPlan
     * 
     * @param other
     * @return
     */
    public Collection<CatalogType> getChangedEntries(PartitionPlan other) {
        Set<CatalogType> changed = new HashSet<CatalogType>();
        changed.addAll(this.getChangedTableEntries(other));
        changed.addAll(this.getChangedProcedureEntries(other));
        return (changed);
    }

    public Collection<Table> getChangedTableEntries(PartitionPlan other) {
        Set<Table> changed = new HashSet<Table>();
        for (Entry<Table, TableEntry> e : this.table_entries.entrySet()) {
            if (!other.table_entries.containsKey(e.getKey())) {
                changed.add(e.getKey());
            } else {
                TableEntry pe0 = e.getValue();
                TableEntry pe1 = other.table_entries.get(e.getKey());
                if (!pe0.equals(pe1))
                    changed.add(e.getKey());
            }
        } // FOR
        return (changed);
    }

    public Collection<Procedure> getChangedProcedureEntries(PartitionPlan other) {
        Set<Procedure> changed = new HashSet<Procedure>();
        for (Entry<Procedure, ProcedureEntry> e : this.proc_entries.entrySet()) {
            if (!other.proc_entries.containsKey(e.getKey())) {
                changed.add(e.getKey());
            } else {
                ProcedureEntry pe0 = e.getValue();
                ProcedureEntry pe1 = other.proc_entries.get(e.getKey());
                if (!pe0.equals(pe1))
                    changed.add(e.getKey());
            }
        } // FOR
        return (changed);
    }

    /**
     * Return the root tables of this PartitionPlan
     * 
     * @return
     */
    public Collection<Table> getRoots() {
        Set<Table> roots = new HashSet<Table>();
        for (Table catalog_tbl : this.table_entries.keySet()) {
            TableEntry entry = this.table_entries.get(catalog_tbl);
            if (entry.getParent() == null)
                roots.add(catalog_tbl);
        } // FOR
        return (roots);
    }

    /**
     * @param catalog_tbl
     * @return
     */
    public Table getRoot(Table catalog_tbl) {
        return (this.table_roots.get(catalog_tbl));
    }

    /**
     * Return the set of non-replicated roots for this PartitionPlan
     * 
     * @return
     */
    public Collection<Table> getNonReplicatedRoots() {
        Set<Table> roots = new HashSet<Table>();
        for (Table catalog_tbl : this.table_entries.keySet()) {
            TableEntry entry = this.table_entries.get(catalog_tbl);
            if (entry.getParent() == null && entry.getMethod() != PartitionMethodType.REPLICATION) {
                roots.add(catalog_tbl);
            }
        } // FOR
        return (roots);
    }

    public Collection<Table> getAncestors(Table catalog_tbl) {
        return (this.table_ancestors.get(catalog_tbl));
    }

    public Collection<Table> getDescendants(Table catalog_tbl) {
        return (this.table_descendants.get(catalog_tbl));
    }

    /**
     * @param catalog_tbl
     * @return
     */
    public Set<Table> getChildren(Table catalog_tbl) {
        // assert(this.table_children.containsKey(catalog_tbl)) :
        // "No entry for " + catalog_tbl;
        return (this.table_children.get(catalog_tbl));
    }

    public Map<CatalogType, CatalogType> toMap() {
        HashMap<CatalogType, CatalogType> map = new HashMap<CatalogType, CatalogType>();
        // TABLES
        for (Entry<Table, TableEntry> e : this.table_entries.entrySet()) {
            switch (e.getValue().getMethod()) {
                case REPLICATION:
                    map.put(e.getKey(), ReplicatedColumn.get(e.getKey()));
                    break;
                case HASH:
                case MAP:
                    map.put(e.getKey(), e.getValue().getAttribute());
                    break;
                default:
                    assert (false) : "Unexpected " + e.getValue().getMethod();
            }

        } // FOR
          // PROCEDURES
        for (Entry<Procedure, ProcedureEntry> e : this.proc_entries.entrySet()) {
            switch (e.getValue().getMethod()) {
                case NONE:
                    map.put(e.getKey(), NullProcParameter.singleton(e.getKey()));
                    break;
                case HASH:
                    map.put(e.getKey(), e.getValue().getAttribute());
                    break;
                default:
                    assert (false) : "Unexpected " + e.getValue().getMethod();
            }
        } // FOR
        return (map);
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof PartitionPlan))
            return (false);
        PartitionPlan other = (PartitionPlan) obj;
        return (this.proc_entries.equals(other.proc_entries) && this.table_entries.equals(other.table_entries));
    }

    public String toString() {
        final int col_width = 30;
        final String format = "%-" + col_width + "s";

        StringBuilder sb = new StringBuilder();
        String labels[] = { "TABLE", "METHOD", "ATTRIBUTE", "MAPPING" };
        for (String label : labels) {
            sb.append(String.format(format, label));
        }

        String single_line = "";
        String double_line = "";
        for (int i = 0, cnt = col_width * labels.length; i < cnt; i++) {
            single_line += "-";
            double_line += "=";
        } // FOR
        sb.append("\n").append(single_line).append("\n");
        for (Table catalog_tbl : this.table_entries.keySet()) {
            TableEntry entry = this.table_entries.get(catalog_tbl);
            assert (entry != null) : "Null PartitionEntry: " + catalog_tbl;
            String mapping = (entry.getParent() != null ? CatalogUtil.getDisplayName(entry.getParentAttribute()) : "<none>");
            sb.append(String.format(format, catalog_tbl.getName())).append(String.format(format, entry.getMethod()))
                    .append(String.format(format, (entry.getAttribute() != null ? entry.getAttribute().getName() : "<none>"))).append(String.format(format, mapping)).append("\n");
        } // FOR
        sb.append(double_line).append("\n");

        labels = new String[] { "PROCEDURE", "METHOD", "ATTRIBUTE", "SINGLEPARTITION" };
        for (String label : labels) {
            sb.append(String.format(format, label));
        }
        sb.append("\n").append(single_line).append("\n");
        for (Procedure catalog_proc : this.proc_entries.keySet()) {
            ProcedureEntry entry = this.proc_entries.get(catalog_proc);
            assert (entry != null) : "Null PartitionEntry: " + catalog_proc;
            sb.append(String.format(format, catalog_proc.getName())).append(String.format(format, entry.getMethod()))
                    .append(String.format(format, (entry.getAttribute() != null ? entry.getAttribute().getName() : "<none>"))).append(String.format(format, entry.isSinglePartition())).append("\n");
        } // FOR

        return (sb.toString());
    }

    // ----------------------------------------------------------------------------
    // STATIC GENERATION METHODS
    // ----------------------------------------------------------------------------

    public static PartitionPlan createFromMap(Map<? extends CatalogType, ? extends CatalogType> pplan_map) {
        return (createFromMap(new PartitionPlan(), pplan_map));
    }

    public static PartitionPlan createFromMap(final PartitionPlan pplan, Map<? extends CatalogType, ? extends CatalogType> pplan_map) {
        assert (pplan_map.isEmpty() == false) : "PartitionPlan map is empty!";
        Database catalog_db = null;

        for (Entry<? extends CatalogType, ? extends CatalogType> e : pplan_map.entrySet()) {
            PartitionMethodType method = null;
            if (e.getValue() != null)
                assert (e.getKey().equals(e.getValue().getParent())) : e;

            // Table Partitioning
            if (e.getKey() instanceof Table) {
                Table catalog_tbl = (Table) e.getKey();
                Column catalog_col = (Column) e.getValue();
                Column attribute = null;
                if (catalog_col instanceof ReplicatedColumn) {
                    method = PartitionMethodType.REPLICATION;
                } else {
                    method = PartitionMethodType.HASH;
                    attribute = catalog_col;
                    assert (catalog_col != null) : "Unexcept Null Partitioning Column: " + catalog_tbl.getName();
                    assert (catalog_col.getParent().equals(e.getKey())) : "Parent mismatch: " + catalog_col.getParent() + " != " + e.getKey();
                }
                TableEntry pentry = new TableEntry(method, attribute, null, null);
                try {
                    pplan.addTablePartitionEntry(catalog_tbl, pentry);
                } catch (AssertionError ex) {
                    LOG.fatal("FAILED: " + e);
                    throw ex;
                }

                // Procedure Partitioning
            } else if (e.getKey() instanceof Procedure) {
                Procedure catalog_proc = (Procedure) e.getKey();
                ProcParameter catalog_proc_param = (ProcParameter) e.getValue();
                boolean single_partition = true;
                ProcParameter attribute = null;

                if (catalog_proc.getSystemproc()) {
                    continue;
                } else if (catalog_proc_param instanceof NullProcParameter || catalog_proc_param == null || catalog_proc.getParameters().size() == 0) {
                    method = PartitionMethodType.NONE;
                    single_partition = false;
                    // attribute =
                    // NullProcParameter.getNullProcParameter(catalog_proc);
                } else {
                    method = PartitionMethodType.HASH;
                    attribute = catalog_proc_param;
                    single_partition = catalog_proc.getSinglepartition();
                    assert (catalog_proc_param != null) : "Unexcept Null ProcParameter: " + catalog_proc;
                    assert (catalog_proc_param.getParent().equals(e.getKey())) : "Parent mismatch: " + catalog_proc_param.getParent() + " != " + e.getKey();
                }
                ProcedureEntry pentry = new ProcedureEntry(method, attribute, single_partition);
                try {
                    pplan.addProcedurePartitionEntry(catalog_proc, pentry);
                } catch (AssertionError ex) {
                    LOG.fatal("FAILED: " + e);
                    throw ex;
                }

                // Unknown!
            } else {
                assert (false) : "Unexpected CatalogType: " + e.getKey();
            }
            catalog_db = CatalogUtil.getDatabase(e.getKey());
        }
        // Then go back and try to resolve foreign key relations for tables
        for (TableEntry pentry : pplan.getTableEntries().values()) {
            if (pentry.isReplicated())
                continue;
            assert (pentry != null);

            // Check whether it is partitioned on a foreign key attribute that
            // has a parent
            // who is not replicate
            Column partition_col = pentry.getAttribute();

            Column parent_col = CatalogUtil.getForeignKeyParent(partition_col);
            if (parent_col != null) {
                pentry.setMethod(PartitionMethodType.MAP);
                pentry.setParent((Table) parent_col.getParent());
                pentry.setParentAttribute(parent_col);
            }
        } // FOR
        assert (catalog_db != null);

        // Construct a partition tree from ourselves and then populate the
        // dependency relationships
        // PartitionTree ptree = PartitionPlanTreeGenerator.generate(catalog_db,
        // pplan);
        // pplan.setPartitionTree(ptree);
        pplan.initializeDependencies();

        return (pplan);

    }

    /**
     * Create a partitioning plan object from a catalog
     * 
     * @param catalog_db
     * @return
     */
    public static PartitionPlan createFromCatalog(Database catalog_db) {
        return (createFromCatalog(catalog_db, null));
    }

    public static PartitionPlan createFromCatalog(Database catalog_db, DesignerHints hints) {
        final Map<CatalogType, CatalogType> pplan_map = new HashMap<CatalogType, CatalogType>();

        // Table Partitioning
        for (Table catalog_tbl : catalog_db.getTables()) {
            if (catalog_tbl.getSystable())
                continue;
            if (catalog_tbl.getIsreplicated()) {
                pplan_map.put(catalog_tbl, ReplicatedColumn.get(catalog_tbl));
            } else {
                Column partition_col = catalog_tbl.getPartitioncolumn();
                pplan_map.put(catalog_tbl, partition_col);
            }
        } // FOR
          // Procedure Partitioning
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (PartitionerUtil.shouldIgnoreProcedure(hints, catalog_proc)) {
                continue;
            } else if (catalog_proc.getPartitionparameter() == NullProcParameter.PARAM_IDX) {
                pplan_map.put(catalog_proc, NullProcParameter.singleton(catalog_proc));
            } else {
                int param_idx = catalog_proc.getPartitionparameter();
                assert (param_idx >= 0);
                ProcParameter catalog_proc_param = catalog_proc.getParameters().get(param_idx);
                assert (catalog_proc_param != null) : "Null ProcParameter for " + catalog_proc.getName() + ": Idx #" + param_idx;
                pplan_map.put(catalog_proc, catalog_proc_param);
            }
        }
        return (PartitionPlan.createFromMap(pplan_map));
    }

    // ----------------------------------------------------------------------------
    // SERIALIZATION METHODS
    // ----------------------------------------------------------------------------

    @Override
    public void load(File input_path, Database catalog_db) throws IOException {
        JSONUtil.load(this, catalog_db, input_path);
    }

    @Override
    public void save(File output_path) throws IOException {
        JSONUtil.save(this, output_path);
    }

    @Override
    public String toJSONString() {
        return (JSONUtil.toJSONString(this));
    }

    @Override
    public void toJSON(JSONStringer stringer) throws JSONException {
        JSONUtil.fieldsToJSON(stringer, this, PartitionPlan.class, PartitionPlan.Members.values());
    }

    @Override
    public void fromJSON(JSONObject json_object, Database catalog_db) throws JSONException {
        JSONUtil.fieldsFromJSON(json_object, catalog_db, this, PartitionPlan.class, PartitionPlan.Members.values());
        this.initializeDependencies();
    }
}