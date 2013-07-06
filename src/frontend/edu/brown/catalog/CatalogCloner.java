package edu.brown.catalog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.CatalogMap;
import org.voltdb.catalog.CatalogProxy;
import org.voltdb.catalog.CatalogType;
import org.voltdb.catalog.Cluster;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Index;
import org.voltdb.catalog.MaterializedViewInfo;
import org.voltdb.catalog.PlanFragment;
import org.voltdb.catalog.ProcParameter;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Site;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.StmtParameter;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.types.ConstraintType;

import edu.brown.catalog.special.MultiColumn;
import edu.brown.catalog.special.MultiProcParameter;
import edu.brown.catalog.special.ReplicatedColumn;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.utils.AbstractTreeWalker;

public abstract class CatalogCloner {
    private static final Logger LOG = Logger.getLogger(CatalogCloner.class);
    private static final LoggerBoolean debug = new LoggerBoolean();
    private static final LoggerBoolean trace = new LoggerBoolean();
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }

    /**
     * Clone and return the given catalog
     * 
     * @param catalog_db
     * @return
     * @throws Exception
     */
    public static Database cloneDatabase(Database catalog_db) throws Exception {
        assert (catalog_db != null);
        // Catalog clone_catalog = new Catalog();
        // clone_catalog.execute(catalog_db.getCatalog().serialize());
        // return (CatalogUtil.getDatabase(clone_catalog));

        final Catalog clone_catalog = CatalogCloner.cloneBaseCatalog(catalog_db.getCatalog(), new ArrayList<Class<? extends CatalogType>>());
        Database clone_db = CatalogUtil.getDatabase(clone_catalog);
        assert (!catalog_db.equals(clone_db));

        // Need to also clone the MultiColumn guys too!
        for (Table catalog_tbl : catalog_db.getTables()) {
            Table clone_tbl = clone_db.getTables().get(catalog_tbl.getName());
            for (Column catalog_col : catalog_tbl.getColumns()) {
                if (catalog_col instanceof MultiColumn) {
                    MultiColumn mc = (MultiColumn) catalog_col;
                    Column clone_cols[] = new Column[mc.size()];
                    for (int i = 0; i < clone_cols.length; i++) {
                        clone_cols[i] = clone_tbl.getColumns().get(mc.get(i).getName());
                    } // FOR

                    MultiColumn clone_mc = MultiColumn.get(clone_cols);
                    assert (clone_mc != null);
                }
            }
            assert (catalog_tbl.getColumns().size() == clone_tbl.getColumns().size()) : catalog_tbl.getColumns() + " != " + clone_tbl.getColumns();
        } // FOR

        // And don't forget MultiProcParameter!
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            Procedure clone_proc = clone_db.getProcedures().get(catalog_proc.getName());
            for (ProcParameter catalog_param : catalog_proc.getParameters()) {
                if (catalog_param instanceof MultiProcParameter) {
                    MultiProcParameter mpp = (MultiProcParameter) catalog_param;
                    ProcParameter clone_params[] = new ProcParameter[mpp.size()];
                    for (int i = 0; i < clone_params.length; i++) {
                        clone_params[i] = clone_proc.getParameters().get(mpp.get(i).getIndex());
                    } // FOR

                    // This will automatically add our guy into clone_tbl
                    MultiProcParameter clone_mpp = MultiProcParameter.get(clone_params);
                    assert (clone_mpp != null);
                }
            }
            assert (catalog_proc.getParameters().size() == clone_proc.getParameters().size()) : catalog_proc.getParameters() + " != " + clone_proc.getParameters();
        } // FOR

        return (clone_db);
    }

    /**
     * Clones the base components of a catalog. All underlying objects are recreated
     * @param catalog
     * @return
     */
    public static Catalog cloneBaseCatalog(Catalog catalog) {
        HashSet<Class<? extends CatalogType>> skip_types = new HashSet<Class<? extends CatalogType>>();
        skip_types.add(Table.class);
        
        // XXX: I don't remember why I did this instead of just re-executing Catalog.serialize()?
        return (CatalogCloner.cloneBaseCatalog(catalog, skip_types));
    }

    public static Catalog cloneBaseCatalog(Catalog catalog, Class<? extends CatalogType>... skip_types) {
        return (CatalogCloner.cloneBaseCatalog(catalog, Arrays.asList(skip_types)));
    }

    /**
     * @param catalog
     * @param skip_types
     * @return
     */
    public static Catalog cloneBaseCatalog(Catalog catalog, final Collection<Class<? extends CatalogType>> skip_types) {
        final Catalog new_catalog = new Catalog();

        new AbstractTreeWalker<CatalogType>() {
            protected void populate_children(AbstractTreeWalker.Children<CatalogType> children, CatalogType element) {
                if (element instanceof Catalog) {
                    children.addAfter(((Catalog) element).getClusters().values());
                } else if (element instanceof Cluster) {
                    children.addAfter(((Cluster) element).getDatabases().values());
                    children.addAfter(((Cluster) element).getHosts().values());
                    children.addAfter(((Cluster) element).getSites().values());
                } else if (element instanceof Site) {
                    children.addAfter(((Site) element).getPartitions().values());
                } else if (element instanceof Database) {
                    children.addAfter(((Database) element).getProcedures().values());
                    children.addAfter(((Database) element).getPrograms().values());
                    children.addAfter(((Database) element).getTables().values());
                } else if (element instanceof Procedure) {
                    for (ProcParameter param : ((Procedure) element).getParameters().values()) {
                        if (!(param instanceof MultiProcParameter))
                            children.addAfter(param);
                    } // FOR
                    children.addAfter(((Procedure) element).getStatements().values());
                } else if (element instanceof Statement) {
                    children.addAfter(((Statement) element).getParameters().values());
                    children.addAfter(((Statement) element).getFragments().values());
                    children.addAfter(((Statement) element).getMs_fragments().values());
                    children.addAfter(((Statement) element).getOutput_columns().values());
                } else if (element instanceof PlanFragment) {
                    // children.addAfter(((PlanFragment)element).getDependencyids().values());
                    // children.addAfter(((PlanFragment)element).getOutputdependencyids().values());
                }
            };

            @Override
            protected void callback(CatalogType element) {
                if (element != null && !skip_types.contains(element.getClass()))
                    CatalogCloner.clone(element, new_catalog);
            }
        }.traverse(catalog);
        
        Database orig_database = CatalogUtil.getDatabase(catalog);
        Database new_database = CatalogUtil.getDatabase(new_catalog);

        if (skip_types.contains(Table.class) == false && skip_types.contains(Column.class) == false) {
            // Clone constraints if they were not skipped
            if (skip_types.contains(Constraint.class) == false) {
                CatalogCloner.cloneConstraints(orig_database, new_database);
            }
            
            // Clone MaterializedViewes if they were not skipped
            // if (skip_types.contains(MaterializedViewInfo.class) == false) {
            // CatalogCloner.cloneViews(CatalogUtil.getDatabase(catalog),
            // CatalogUtil.getDatabase(new_catalog));
            // }
        }
            
        // Clone Procedure conflicts if they were not skipped
        if (skip_types.contains(Procedure.class) == false) {
            CatalogCloner.cloneConflicts(orig_database, new_database);
        }
            
        return (new_catalog);
    }

    /**
     * Add a single catalog element from one catalog into the destination
     * catalog Note that this will not copy constraints for tables, since that
     * needs to be done later to ensure that any foreign key references are
     * included properly
     * 
     * @param <T>
     * @param src_item
     * @param dest_catalog
     * @return
     */
    @SuppressWarnings("unchecked")
    public static <T extends CatalogType> T clone(T src_item, Catalog dest_catalog) {
        StringBuilder buffer = new StringBuilder();
        if (src_item instanceof MultiProcParameter) {
            LOG.warn(src_item + ": ??????????");
            return (null);
        }
        CatalogProxy.writeCommands(src_item, buffer);
        dest_catalog.execute(buffer.toString());
        T clone = (T) dest_catalog.getItemForRef(src_item.getPath());

        // SPECIAL HANDLING

        // Table
        if (src_item instanceof Table) {
            Table src_tbl = (Table) src_item;
            Table dest_tbl = (Table) clone;

            // Columns
            for (Column src_col : src_tbl.getColumns()) {
                if (!(src_col instanceof MultiColumn))
                    CatalogCloner.clone(src_col, dest_catalog);
            } // FOR
            // Indexes
            for (Index src_idx : src_tbl.getIndexes()) {
                CatalogCloner.clone(src_idx, dest_catalog);
            } // FOR
            // MaterializedViews
            for (MaterializedViewInfo src_view : src_tbl.getViews()) {
                CatalogCloner.clone(src_view, dest_catalog);
            } // FOR

            // // Constraints
            // for (Constraint src_cons : ((Table)src).getConstraints()) {
            // CatalogUtil.clone(src_cons, dest_catalog);
            // } // FOR

            // Partitioning Column
            if (src_tbl.getPartitioncolumn() != null) {
                Column src_part_col = src_tbl.getPartitioncolumn();
                Column dest_part_col = null;

                // Special Case: Replicated Column Marker
                if (src_part_col instanceof ReplicatedColumn) {
                    dest_part_col = ReplicatedColumn.get(dest_tbl);
                // Special Case: MultiColumn
                } else if (src_part_col instanceof MultiColumn) {
                    MultiColumn mc = (MultiColumn) src_part_col;
                    Column dest_cols[] = new Column[mc.size()];
                    for (int i = 0; i < dest_cols.length; i++) {
                        dest_cols[i] = dest_tbl.getColumns().get(mc.get(i).getName());
                    } // FOR
                    dest_part_col = MultiColumn.get(dest_cols);

                } else {
                    dest_part_col = dest_tbl.getColumns().get(src_part_col.getName());
                }
                assert (dest_part_col != null) : "Missing partitioning column " + CatalogUtil.getDisplayName(src_part_col);
                dest_tbl.setPartitioncolumn(dest_part_col);
            }
        // MaterializedViewInfo
        } else if (src_item instanceof MaterializedViewInfo) {
            // ColumnRefs
            MaterializedViewInfo src_view = (MaterializedViewInfo) src_item;
            MaterializedViewInfo dest_view = (MaterializedViewInfo) clone;
            updateColumnsRefs((Table) src_view.getParent(), src_view.getGroupbycols(), (Table) dest_view.getParent(), dest_view.getGroupbycols());

        // Index
        } else if (src_item instanceof Index) {
            // ColumnRefs
            Index src_idx = (Index) src_item;
            Index dest_idx = (Index) clone;
            updateColumnsRefs((Table) src_idx.getParent(), src_idx.getColumns(), (Table) dest_idx.getParent(), dest_idx.getColumns());

        // Constraint
        } else if (src_item instanceof Constraint) {
            // ColumnRefs
            Constraint src_cons = (Constraint) src_item;
            Constraint dest_cons = (Constraint) clone;

            Table src_fkey_tbl = src_cons.getForeignkeytable();
            if (src_fkey_tbl != null) {
                Database dest_db = (Database) dest_cons.getParent().getParent();
                Table dest_fkey_tbl = dest_db.getTables().get(src_fkey_tbl.getName());
                if (dest_fkey_tbl != null) {
                    dest_cons.setForeignkeytable(dest_fkey_tbl);
                    for (ColumnRef src_cref : ((Constraint) src_item).getForeignkeycols()) {
                        CatalogCloner.clone(src_cref, dest_catalog);

                        // Correct what it's pointing to
                        ColumnRef dest_colref = dest_cons.getForeignkeycols().get(src_cref.getName());
                        assert (dest_colref != null);
                        dest_colref.setColumn(dest_fkey_tbl.getColumns().get(src_cref.getColumn().getName()));
                    } // FOR
                }
            }

            // Important: We have to add ConstraintRefs to Columns *after* we add the columns
            Table src_tbl = (Table) src_cons.getParent();
            Table dest_tbl = (Table) dest_cons.getParent();
            for (Column src_col : src_tbl.getColumns()) {
                Column dest_col = dest_tbl.getColumns().get(src_col.getName());
                assert (dest_col != null);
                for (ConstraintRef src_conref : src_col.getConstraints()) {
                    if (!src_conref.getConstraint().equals(src_cons))
                        continue;
                    CatalogCloner.clone(src_conref, dest_catalog);

                    // Correct what it's pointing to
                    ConstraintRef dest_conref = dest_col.getConstraints().get(src_conref.getName());
                    assert (dest_conref != null);
                    // System.out.println("dest_tbl: " + dest_tbl);
                    // System.out.println("dest_tbl.getConstraints(): " +
                    // CatalogUtil.debug(dest_tbl.getConstraints()));
                    // System.out.println("src_confref: " + src_conref);
                    // System.out.println("src_confref.getConstraint(): " +
                    // src_conref.getConstraint());
                    dest_conref.setConstraint(dest_tbl.getConstraints().get(src_conref.getConstraint().getName()));
                } // FOR
            } // FOR

            Index src_index = src_cons.getIndex();
            if (src_index != null) {
                Index dest_index = dest_tbl.getIndexes().get(src_index.getName());
                dest_cons.setIndex(dest_index);
            }

        // StmtParameter
        } else if (src_item instanceof StmtParameter) {
            // We need to fix the reference to the ProcParameter (if one exists)
            StmtParameter src_stmt_param = (StmtParameter) src_item;
            StmtParameter dest_stmt_param = (StmtParameter) clone;

            if (src_stmt_param.getProcparameter() != null) {
                Procedure dest_proc = (Procedure) dest_stmt_param.getParent().getParent();
                ProcParameter src_proc_param = src_stmt_param.getProcparameter();
                ProcParameter dest_proc_param = dest_proc.getParameters().get(src_proc_param.getName());
                if (dest_proc_param == null) {
                    LOG.warn("dest_proc:      " + dest_proc);
                    LOG.warn("dest_stmt:      " + dest_stmt_param.getParent());
                    LOG.warn("src_proc_param: " + src_proc_param);
                    LOG.warn("dest_proc.getParameters(): " + CatalogUtil.debug(dest_proc.getParameters()));
                    CatalogUtil.saveCatalog(dest_catalog, CatalogUtil.CATALOG_FILENAME);
                }

                assert (dest_proc_param != null);
                dest_stmt_param.setProcparameter(dest_proc_param);
            }
        }
        return (clone);
    }

    private static void updateColumnsRefs(Table src_tbl, CatalogMap<ColumnRef> src_refs, Table dest_tbl, CatalogMap<ColumnRef> dest_refs) {
        for (ColumnRef src_colref : src_refs) {
            // First clone it
            CatalogCloner.clone(src_colref, dest_tbl.getCatalog());

            // Correct what it's pointing to
            ColumnRef dest_colref = dest_refs.get(src_colref.getName());
            dest_colref.setColumn(dest_tbl.getColumns().get(src_colref.getColumn().getName()));
        } // FOR
    }
    
    /**
     * Clone Procedure conflicts
     * @param src_db
     * @param dest_db
     */
    public static void cloneConflicts(Database src_db, Database dest_db) {
        for (Procedure src_proc : src_db.getProcedures()) {
            Procedure dest_proc = dest_db.getProcedures().get(src_proc.getName());
            assert(dest_proc != null) : src_proc;
            
            for (ConflictSet src_conflicts : src_proc.getConflicts()) {
                Procedure dest_otherProc = dest_db.getProcedures().get(src_conflicts.getProcedure().getName());
                ConflictSet dest_conflicts = dest_proc.getConflicts().add(src_conflicts.getName());
                dest_conflicts.setProcedure(dest_otherProc);
                
                for (ConflictPair src_pair : src_conflicts.getReadwriteconflicts()) {
                    ConflictPair dest_pair = clone(src_pair, dest_db.getCatalog());
                    dest_pair.setStatement0(dest_proc.getStatements().get(src_pair.getStatement0().getName()));
                    dest_pair.setStatement1(dest_otherProc.getStatements().get(src_pair.getStatement1().getName()));
                    for (TableRef src_ref : src_pair.getTables()) {
                        TableRef dest_ref = dest_pair.getTables().add(src_ref.getName());
                        dest_ref.setTable(dest_db.getTables().get(src_ref.getTable().getName()));
                    } // FOR
                } // FOR
                for (ConflictPair src_pair : src_conflicts.getWritewriteconflicts()) {
                    ConflictPair dest_pair = clone(src_pair, dest_db.getCatalog());
                    dest_pair.setStatement0(dest_proc.getStatements().get(src_pair.getStatement0().getName()));
                    dest_pair.setStatement1(dest_otherProc.getStatements().get(src_pair.getStatement1().getName()));
                    for (TableRef src_ref : src_pair.getTables()) {
                        TableRef dest_ref = dest_pair.getTables().add(src_ref.getName());
                        dest_ref.setTable(dest_db.getTables().get(src_ref.getTable().getName()));
                    } // FOR
                } // FOR
            }
        } // FOR
    }

    /**
     * @param src_db
     * @param dest_db
     */
    public static void cloneConstraints(Database src_db, Database dest_db) {
        Catalog dest_catalog = dest_db.getCatalog();
        for (Table src_tbl : src_db.getTables()) {
            Table dest_tbl = dest_db.getTables().get(src_tbl.getName());
            if (dest_tbl != null) {
                for (Constraint src_cons : src_tbl.getConstraints()) {
                    // Only clone FKEY constraint if the other table is in the catalog
                    ConstraintType cons_type = ConstraintType.get(src_cons.getType());
                    if (cons_type != ConstraintType.FOREIGN_KEY || (cons_type == ConstraintType.FOREIGN_KEY && dest_db.getTables().get(src_cons.getForeignkeytable().getName()) != null)) {
                        Constraint dest_cons = clone(src_cons, dest_catalog);
                        assert (dest_cons != null);
                    }
                } // FOR
            }
        } // FOR
    }

}
