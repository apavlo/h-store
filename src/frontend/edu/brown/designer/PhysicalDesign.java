package edu.brown.designer;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.Table;
import org.voltdb.types.IndexType;

import edu.brown.catalog.CatalogKey;
import edu.brown.designer.partitioners.plan.PartitionEntry;
import edu.brown.designer.partitioners.plan.PartitionPlan;

public class PhysicalDesign {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(PhysicalDesign.class.getName());

    protected final Database catalog_db;
    public PartitionPlan plan;
    public final IndexPlan indexes = null; // new IndexPlan(null);
    private final Map<String, PartitionEntry> table_partitions = new HashMap<String, PartitionEntry>();

    public PhysicalDesign(Database catalog_db) {
        this.catalog_db = catalog_db;
    }

    public Map<String, PartitionEntry> getTablePartitions() {
        return table_partitions;
    }

    public void addTablePartition(Table catalog_tbl, PartitionEntry entry) {
        this.table_partitions.put(CatalogKey.createKey(catalog_tbl), entry);
    }

    public PartitionEntry getTablePartition(Table catalog_tbl) {
        return (this.table_partitions.get(CatalogKey.createKey(catalog_tbl)));
    }

    public String debug() {
        StringBuilder buffer = new StringBuilder();

        String delimiter = "\t";
        String labels[] = { "TABLE", "METHOD", "ATTRIBUTES", "MAPPING" };
        String add = "";
        for (String label : labels) {
            buffer.append(add).append(label);
            add = delimiter;
        } // FOR
        buffer.append("\n");

        // for (Table catalog_tbl : this.table_partitions.keySet()) {
        // buffer.append(entry.getTable()).append(delimiter).append(entry.toString(delimiter)).append("\n");
        // }
        //
        // for (PartitionEntry entry : this.table_partitions.values()) {
        //
        // } // FOR
        return (buffer.toString());

    }

    public Catalog createCatalog() throws Exception {
        Catalog new_catalog = new Catalog();
        new_catalog.execute(this.catalog_db.getCatalog().serialize());
        Database new_catalog_db = new_catalog.getClusters().get(this.catalog_db.getParent().getName()).getDatabases().get(this.catalog_db.getName());

        //
        // First apply the partitioning plan to all the tables
        //
        for (Table catalog_tbl : this.plan.getTableEntries().keySet()) {
            PartitionEntry entry = this.plan.getTableEntries().get(catalog_tbl);
            Table new_catalog_tbl = new_catalog_db.getTables().get(catalog_tbl.getName());
            switch (entry.getMethod()) {
                case REPLICATION:
                    new_catalog_tbl.setIsreplicated(true);
                    break;
                case HASH:
                case MAP:
                    new_catalog_tbl.setIsreplicated(false);
                    Column new_catalog_col = new_catalog_tbl.getColumns().get(entry.getAttribute().getName());
                    new_catalog_tbl.setPartitioncolumn(new_catalog_col);
                    break;
                default:
                    LOG.fatal("Unsupported partition type '" + entry.getMethod() + "'");
                    System.exit(1);
            } // SWITCH
        } // FOR

        //
        // Then add all our of our indexes
        //
        Map<Table, Integer> table_idxs = new HashMap<Table, Integer>();
        for (Table catalog_tbl : this.indexes.keySet()) {
            for (IndexPlan.Entry index : this.indexes.get(catalog_tbl)) {
                Table new_catalog_tbl = new_catalog_db.getTables().get(catalog_tbl.getName());
                if (!table_idxs.containsKey(new_catalog_tbl)) {
                    table_idxs.put(new_catalog_tbl, 0);
                }
                int idx = table_idxs.get(new_catalog_tbl);
                table_idxs.put(new_catalog_tbl, idx + 1);

                // TODO: Support different index types
                String idx_name = "IDX_" + new_catalog_tbl.getName() + "_DESIGNER_" + idx;
                Index new_catalog_index = catalog_tbl.getIndexes().add(idx_name);
                new_catalog_index.setType(IndexType.HASH_TABLE.getValue());

                // need to set other index data here (column, etc)
                for (int i = 0, cnt = index.getColumns().size(); i < cnt; i++) {
                    Column catalog_col = index.getColumns().get(i);
                    Column new_catalog_col = new_catalog_tbl.getColumns().get(catalog_col.getName());
                    ColumnRef cref = new_catalog_index.getColumns().add(new_catalog_col.getName());
                    cref.setColumn(new_catalog_col);
                    cref.setIndex(i);
                } // FOR
            } // FOR
        } // FOR

        //
        // XXX: For now we set all the procedures to be non-singlesited
        //
        /*
         * for (Procedure catalog_proc : this.catalog_db.getProcedures()) {
         * catalog_proc.setPartitioncolumn(null);
         * catalog_proc.setPartitionparameter(-1);
         * catalog_proc.setPartitiontable(null); } // FOR
         */
        return (new_catalog);
    }

}
