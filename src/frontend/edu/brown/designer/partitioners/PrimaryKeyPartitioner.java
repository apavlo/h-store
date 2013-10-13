package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;

import edu.brown.catalog.CatalogKey;
import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.ProcedureEntry;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;

public class PrimaryKeyPartitioner extends AbstractPartitioner {
    private static final Logger LOG = Logger.getLogger(PrimaryKeyPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    public PrimaryKeyPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {
        PartitionPlan pplan = new PartitionPlan();

        if (debug.val)
            LOG.debug("Selecting partitioning Column for " + this.info.catalogContext.database.getTables().size() + " Tables");
        double total_memory_used = 0.0;
        boolean calculate_memory = (hints.force_replication_size_limit != null && hints.max_memory_per_partition != 0);
        for (Table catalog_tbl : CatalogUtil.getDataTables(info.catalogContext.database)) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            TableEntry pentry = null;
            Column col = null;
            Collection<Column> pkey_columns = CatalogUtil.getPrimaryKeyColumns(catalog_tbl);

            TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
            assert (ts != null) : "Null TableStatistics for " + catalog_tbl;
            double size_ratio = (calculate_memory ? (ts.tuple_size_total / (double) hints.max_memory_per_partition) : 0);

            // Replication
            if (hints.force_replication.contains(table_key) || (calculate_memory && ts.readonly && size_ratio <= hints.force_replication_size_limit) || pkey_columns.isEmpty()) {
                total_memory_used += size_ratio;
                if (debug.val)
                    LOG.debug("Choosing " + catalog_tbl.getName() + " for replication");
                col = ReplicatedColumn.get(catalog_tbl);
                pentry = new TableEntry(PartitionMethodType.REPLICATION, col, null, null);

                // Hash Primary Key
            } else {
                total_memory_used += (size_ratio / (double) info.getNumPartitions());

                if (hints.enable_multi_partitioning == false || pkey_columns.size() == 1) {
                    col = CollectionUtil.first(pkey_columns);
                    pentry = new TableEntry(PartitionMethodType.HASH, col, null, null);
                } else {
                    col = MultiColumn.get(pkey_columns.toArray(new Column[0]));
                    pentry = new TableEntry(PartitionMethodType.HASH, col, null, null);
                }
                assert (pentry.attribute != null) : catalog_tbl;
            }
            if (debug.val)
                LOG.debug(String.format("[%02d] %s", pplan.getTableCount(), col.fullName()));
            pplan.getTableEntries().put(catalog_tbl, pentry);
        } // FOR
        assert (total_memory_used <= 100) : "Too much memory per partition: " + total_memory_used;

        if (hints.enable_procparameter_search) {
            if (debug.val)
                LOG.debug("Selecting partitioning ProcParameter for " + this.info.catalogContext.database.getProcedures().size() + " Procedures");
            pplan.apply(info.catalogContext.database);
            for (Procedure catalog_proc : this.info.catalogContext.database.getProcedures()) {
                if (catalog_proc.getSystemproc() || catalog_proc.getParameters().size() == 0)
                    continue;
                Set<String> param_order = PartitionerUtil.generateProcParameterOrder(info, info.catalogContext.database, catalog_proc, hints);
                ProcedureEntry pentry = new ProcedureEntry(PartitionMethodType.HASH, CatalogKey.getFromKey(info.catalogContext.database, CollectionUtil.first(param_order), ProcParameter.class), null);
                pplan.getProcedureEntries().put(catalog_proc, pentry);
            } // FOR
        }
        this.setProcedureSinglePartitionFlags(pplan, hints);

        return (pplan);
    }
}