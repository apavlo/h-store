package edu.brown.designer.partitioners;

import java.util.Collection;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Table;
import org.voltdb.types.PartitionMethodType;
import org.voltdb.utils.CatalogUtil;

import edu.brown.catalog.CatalogKey;
import edu.brown.designer.Designer;
import edu.brown.designer.DesignerHints;
import edu.brown.designer.DesignerInfo;
import edu.brown.designer.partitioners.plan.PartitionPlan;
import edu.brown.designer.partitioners.plan.ProcedureEntry;
import edu.brown.designer.partitioners.plan.TableEntry;
import edu.brown.statistics.TableStatistics;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.LoggerUtil;
import edu.brown.utils.LoggerUtil.LoggerBoolean;

public class PrimaryKeyPartitioner extends AbstractPartitioner {
    private static final Logger LOG = Logger.getLogger(PrimaryKeyPartitioner.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    public PrimaryKeyPartitioner(Designer designer, DesignerInfo info) {
        super(designer, info);
    }

    @Override
    public PartitionPlan generate(DesignerHints hints) throws Exception {
        PartitionPlan pplan = new PartitionPlan();
        
        if (debug.get()) LOG.debug("Selecting partitioning Column for " + this.info.catalog_db.getTables().size() + " Tables");
        double total_memory_used = 0.0;
        boolean calculate_memory = (hints.force_replication_size_limit != null && hints.max_memory_per_partition != 0);
        for (Table catalog_tbl : info.catalog_db.getTables()) {
            String table_key = CatalogKey.createKey(catalog_tbl);
            TableEntry pentry = null;
            Collection<Column> pkey_columns = CatalogUtil.getPrimaryKeyColumns(catalog_tbl);
            
            TableStatistics ts = info.stats.getTableStatistics(catalog_tbl);
            assert(ts != null) : "Null TableStatistics for " + catalog_tbl;
            double size_ratio = (calculate_memory ? (ts.tuple_size_total / (double)hints.max_memory_per_partition) : 0);
            
            // Replication
            if (hints.force_replication.contains(table_key) ||
                (calculate_memory && ts.readonly && size_ratio <= hints.force_replication_size_limit) ||
                pkey_columns.isEmpty()) {
                total_memory_used += size_ratio;
                if (debug.get()) LOG.debug("Choosing " + catalog_tbl.getName() + " for replication");
                pentry = new TableEntry(PartitionMethodType.REPLICATION, null, null, null);
                
            // Hash Primary Key
            // If the table has multiple primary keys, just pick the first one for now
            } else {
                total_memory_used += (size_ratio / (double)info.getNumPartitions());
                for (Column catalog_col : pkey_columns) {
                    pentry = new TableEntry(PartitionMethodType.HASH, catalog_col, null, null);
                    break;
                } // FOR
            }
            pplan.getTableEntries().put(catalog_tbl, pentry);
        } // FOR
        assert(total_memory_used <= 100) : "Too much memory per partition: " + total_memory_used;
        
        if (hints.enable_procparameter_search) {
            if (debug.get()) LOG.debug("Selecting partitioning ProcParameter for " + this.info.catalog_db.getProcedures().size() + " Procedures");
            pplan.apply(info.catalog_db);
            for (Procedure catalog_proc : this.info.catalog_db.getProcedures()) {
                if (catalog_proc.getSystemproc() || catalog_proc.getParameters().size() == 0) continue;
                Set<String> param_order = PartitionerUtil.generateProcParameterOrder(info, info.catalog_db, catalog_proc, hints);
                ProcedureEntry pentry = new ProcedureEntry(PartitionMethodType.HASH, CatalogKey.getFromKey(info.catalog_db, CollectionUtil.first(param_order), ProcParameter.class), null);
                pplan.getProcedureEntries().put(catalog_proc, pentry);
            } // FOR
        }
        this.setProcedureSinglePartitionFlags(pplan, hints);
        
        return (pplan);
    }
}