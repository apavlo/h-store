package edu.brown.gui.catalog;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Host;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.Partition;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.CatalogType.UnresolvedInfo;
import org.voltdb.plannodes.AbstractPlanNode;
import org.voltdb.types.ConstraintType;
import org.voltdb.types.IndexType;
import org.voltdb.types.QueryType;
import org.voltdb.utils.Encoder;

import edu.brown.catalog.CatalogUtil;
import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;
import edu.brown.utils.CollectionUtil;
import edu.brown.utils.SQLFormatter;
import edu.brown.utils.StringUtil;

/**
 * Catalog Attributes Pretty Printer
 * @author pavlo
 */
public class CatalogAttributeText {
    private static final Logger LOG = Logger.getLogger(CatalogAttributeText.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    final Catalog catalog;
    
    public CatalogAttributeText(Catalog catalog) {
        this.catalog = catalog;
    }
 
    /**
     * Generate a formatted print out of the attributes for a given catalog object 
     * @param catalog_obj
     */
    @SuppressWarnings("unchecked")
    public String getAttributesText(CatalogType catalog_obj) {
        Map<String, Object> m[] = (Map<String, Object>[])new Map[1];
        m[0] = new LinkedHashMap<String, Object>();
        
//      StringBuilder buffer = new StringBuilder();
        // buffer.append("guid: ").append(catalog_obj.getGuid()).append("\n");
        m[0].put("relativeIndex", catalog_obj.getRelativeIndex());
        m[0].put("nodeVersion", catalog_obj.getNodeVersion());
        
        // Default Output
        if ((catalog_obj instanceof Database) == false) {
            Collection<String> skip_fields = CollectionUtil.addAll(new HashSet<String>(),
                    "exptree", "fullplan", "ms_exptree", "ms_fullplan", "plannodetree", "sqltext");
            
            Collection<String> catalog_fields = CollectionUtil.addAll(new HashSet<String>(),
                    "partition_column", "partitioncolumn", "foreignkeytable", "matviewsource"); 
            
            if (catalog_obj instanceof Constraint) {
                catalog_fields.add("index");
            } else if (catalog_obj instanceof Site) {
                catalog_fields.add("host");
                catalog_fields.add("partition");
            }
            
            // Show catalog type
            Set<Class<? extends CatalogType>> show_type = new HashSet<Class<? extends CatalogType>>();
            show_type.add(Host.class);
            show_type.add(Site.class);
            show_type.add(Partition.class);
            
            for (String field : catalog_obj.getFields()) {
                
                if (skip_fields.contains(field)) {
                    if (LOG.isDebugEnabled())
                        LOG.warn(String.format("Skipping %s.%s", catalog_obj.getClass().getSimpleName(), field));
                    continue;
                }
                
                // Default
                Object value = catalog_obj.getField(field);
                m[0].put(field, value);
                
                // Specialized Output
                if (value != null && catalog_fields.contains(field)) {
                    CatalogType catalog_item = null;
                    if (value instanceof CatalogType) {
                        catalog_item = (CatalogType)value;
                    } else if (value instanceof CatalogType.UnresolvedInfo) {
                        catalog_item = catalog.getItemForRef(((UnresolvedInfo)value).path);
                    } else {
                        assert(false) : "Unexpected value '" + value + "' for field '" + field + "'";
                    }
                    
                    if (catalog_item != null) {
                        boolean include_class = show_type.contains(catalog_item.getClass()); 
                        m[0].put(field, CatalogUtil.getDisplayName(catalog_item, include_class));
                    } else {
                        m[0].put(field, catalog_item);
                    }
                }
                
                // CONSTRAINT
                else if (catalog_obj instanceof Constraint) {
                    if (field == "type") {
                        m[0].put(field, ConstraintType.get((Integer)value));
                    }
                }
                // INDEX
                else if (catalog_obj instanceof Index) {
                    if (field == "type") {
                        m[0].put(field, IndexType.get((Integer)value));
                    }
                }
                // STATEMENT
                else if (catalog_obj instanceof Statement) {
                    if (field == "querytype") {
                        m[0].put(field, QueryType.get((Integer)value));
                    }
                }
                // COLUMN / STMTPARAMETER / PROCPARAMETER
                else if (catalog_obj instanceof Column || catalog_obj instanceof StmtParameter || catalog_obj instanceof ProcParameter) {
                    String keys[] = { "type", "sqltype", "javatype", "defaultvaluetype" };
                    for (String key : keys) {
                        if (field == key) {
                            m[0].put(field, VoltType.get(((Integer)value).byteValue()).name());
                            break;
                        }
                    } // FOR
                    if (field.equals("procparameter")) {
                        ProcParameter proc_param = ((StmtParameter)catalog_obj).getProcparameter();
                        if (proc_param != null) {
                            m[0].put(field, proc_param.fullName()); 
                        }
                    }
                }
            } // FOR
        } else {
            m[0].put("project", ((Database)catalog_obj).getProject());
        }
        
        // INDEX
        if (catalog_obj instanceof Index) {
            Index catalog_idx = (Index)catalog_obj;
            Collection<Column> cols = CatalogUtil.getColumns(CatalogUtil.getSortedCatalogItems(catalog_idx.getColumns(), "index"));
            m[0].put("columns", CatalogUtil.getDisplayNames(cols));
        }
        // CONSTRAINT
        else if (catalog_obj instanceof Constraint) {
            Constraint catalog_const = (Constraint)catalog_obj;
            Collection<Column> cols = null;
            if (catalog_const.getType() == ConstraintType.FOREIGN_KEY.getValue()) {
                cols = CatalogUtil.getColumns(catalog_const.getForeignkeycols());    
            } else {
                Index catalog_idx = catalog_const.getIndex();
                cols = CatalogUtil.getColumns(catalog_idx.getColumns());
            }
            m[0].put("columns", CatalogUtil.getDisplayNames(cols));
        }
        // COLUMN
        else if (catalog_obj instanceof Column) {
            Column catalog_col = (Column)catalog_obj;
            Collection<Constraint> consts = CatalogUtil.getConstraints(catalog_col.getConstraints());
            m[0].put("constraints", CatalogUtil.getDisplayNames(consts));
        }
        // MATERIALIZEDVIEWINFO
        else if (catalog_obj instanceof MaterializedViewInfo) {
            MaterializedViewInfo catalog_view = (MaterializedViewInfo)catalog_obj;
            Collection<ColumnRef> cols = catalog_view.getGroupbycols();
            m[0].put("groupbycols", CatalogUtil.debug(CatalogUtil.getColumns(cols)));
        }
        // PROCEDURE
        else if (catalog_obj instanceof Procedure) {
            Procedure catalog_proc = (Procedure)catalog_obj;
            Collection<Procedure> rwConflicts = ConflictSetUtil.getReadWriteConflicts(catalog_proc);
            Collection<Procedure> wwConflicts = ConflictSetUtil.getWriteWriteConflicts(catalog_proc);
            
            if (rwConflicts.size() > 0 || wwConflicts.size() > 0) {
                Map<String, Object> orig_m = m[0];
                m = (Map<String, Object>[])new Map[2];
                m[0] = orig_m;
                
                m[1] = new TreeMap<String, Object>();
                Collection<Procedure> conflicts[] = (Collection<Procedure>[])new Collection<?>[] {
                    rwConflicts,
                    wwConflicts
                };
                String labels[] = { "Read-Write", "Write-Write" };
                
                for (int i = 0; i < labels.length; i++) {
                    String value = "";
                    
                    Collection<Procedure> c = conflicts[i];
                    if (c.size() > 0) {
                        List<String> conflictLabels = new ArrayList<String>(CatalogUtil.getDisplayNames(rwConflicts));
                        Collections.sort(conflictLabels);
                        value = StringUtil.join("\n", conflictLabels);
                    } else {
                        value ="<NONE>";
                    }
                    m[1].put(labels[i] + " Conflicts", value + "\n");
                } // FOR
            }
        }
        
        StringBuilder sb = new StringBuilder(StringUtil.formatMaps(m));
        
        // DATABASE
        if (catalog_obj instanceof Database) {
            sb.append(StringUtil.SINGLE_LINE);
            sb.append(Encoder.hexDecodeToString(((Database)catalog_obj).getSchema()));
        }
        // PLANFRAGMENT
        else if (catalog_obj instanceof PlanFragment) {
            PlanFragment catalog_frgmt = (PlanFragment)catalog_obj;
            try {
                AbstractPlanNode node = PlanNodeUtil.getPlanNodeTreeForPlanFragment(catalog_frgmt);
                sb.append(StringUtil.SINGLE_LINE);
                sb.append(PlanNodeUtil.debug(node));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        // TABLE
        else if (catalog_obj instanceof Table) {
            sb.append(StringUtil.SINGLE_LINE);
            Table catalog_tbl = (Table)catalog_obj;
            
            // MATERIALIZED VIEW
            if (catalog_tbl.getMaterializer() != null) {
                Table parent = catalog_tbl.getMaterializer();
                MaterializedViewInfo catalog_view = parent.getViews().get(catalog_tbl.getName());
                assert(catalog_view != null) :
                    "Unexpected null MaterializedViewInfo '" + catalog_tbl.getName() + "'";
                sb.append(MaterializedViewInfo.class.getSimpleName()).append("\n");
                sb.append(this.getAttributesText(catalog_view));
                
                SQLFormatter f = new SQLFormatter(catalog_view.getSqltext());
                sb.append(StringUtil.SINGLE_LINE);
                sb.append("\n").append(f.format()).append("\n");
            } else {
                String schema = CatalogUtil.toSchema(catalog_tbl);
                sb.append("\n").append(schema).append("\n");
            }
        }
        // Statement
        else if (catalog_obj instanceof Statement) {
            Statement catalog_stmt = (Statement)catalog_obj;
            SQLFormatter f = new SQLFormatter(catalog_stmt.getSqltext());
            sb.append(StringUtil.SINGLE_LINE);
            sb.append("\n").append(f.format()).append("\n");
        }

        return (sb.toString());
    }
    
}
