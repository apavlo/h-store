package edu.brown.catalog.conflicts;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections15.CollectionUtils;
import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ConflictSet;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.catalog.Table;
import org.voltdb.catalog.TableRef;
import org.voltdb.types.QueryType;

import edu.brown.catalog.CatalogUtil;
import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;
import edu.brown.plannodes.PlanNodeUtil;

/**
 * Procedure ConflictSet Calculator
 * @author pavlo
 */
public class ConflictSetCalculator {
    private static final Logger LOG = Logger.getLogger(ConflictSetCalculator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private static class ProcedureInfo {
        final Procedure proc;
        
        final Set<Statement> readQueries = new HashSet<Statement>();
        final Set<Statement> writeQueries = new HashSet<Statement>();
        
        final Map<Procedure, Collection<Table>> rwConflicts = new HashMap<Procedure, Collection<Table>>();
        final Map<Procedure, Collection<Table>> wwConflicts = new HashMap<Procedure, Collection<Table>>();
        
        ProcedureInfo(Procedure proc) {
            this.proc = proc;
        }
    }
    
    private final Database catalog_db;
    private final LinkedHashMap<Procedure, ProcedureInfo> procedures = new LinkedHashMap<Procedure, ConflictSetCalculator.ProcedureInfo>(); 
    private final Set<Procedure> ignoredProcedures = new HashSet<Procedure>();
    private final Set<Statement> ignoredStatements = new HashSet<Statement>();
    
    public ConflictSetCalculator(Catalog catalog) {
        this.catalog_db = CatalogUtil.getDatabase(catalog);

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getMapreduce() || catalog_proc.getSystemproc()) continue;
            if (this.ignoredProcedures.contains(catalog_proc)) continue;
            
            ProcedureInfo pInfo = new ProcedureInfo(catalog_proc);
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (this.ignoredStatements.contains(catalog_stmt)) continue;
                
                if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
                    pInfo.readQueries.add(catalog_stmt);
                } else {
                    pInfo.writeQueries.add(catalog_stmt);
                }
            } // FOR (statement)
            this.procedures.put(catalog_proc, pInfo);
        } // FOR (procedure)
    }
    
    /**
     * Add a Procedure to be completely ignored when processing conflicts
     * @param proc
     */
    public void ignoreProcedure(Procedure proc) {
        this.ignoredProcedures.add(proc);
    }
    
    /**
     * Add a Statement to be completely ignored when processing conflicts
     * @param stmt
     */
    public void ignoreStatement(Statement stmt) {
        this.ignoredStatements.add(stmt);
    }
    
    public void process() throws Exception {
        Collection<Table> conflicts = null;
        
        // For each Procedure, get the list of read and write queries
        // Then check whether there is a R-W or W-R conflict with other procedures
        for (Procedure proc0 : this.procedures.keySet()) {
            if (this.ignoredProcedures.contains(proc0)) continue;
            
            for (Procedure proc1 : this.procedures.keySet()) {
                if (this.ignoredProcedures.contains(proc1)) continue;
                // if (proc0.equals(proc1)) continue;
                
                conflicts = this.checkReadWriteConflict(proc0, proc1);
                if (conflicts.isEmpty() == false) {
                    if (debug.get()) 
                        LOG.debug(String.format("**RW-CONFLICT** %s <-> %s", proc0.getName(), proc1.getName()));
                    this.procedures.get(proc0).rwConflicts.put(proc1, conflicts);
                }
                
                conflicts = this.checkWriteWriteConflict(proc0, proc1);
                if (conflicts.isEmpty() == false) {
                    if (debug.get()) 
                        LOG.debug(String.format("**WW-CONFLICT** %s <-> %s", proc0.getName(), proc1.getName()));
                    this.procedures.get(proc0).wwConflicts.put(proc1, conflicts);
                }
            } // FOR
        } // FOR
        
        for (Procedure proc : this.procedures.keySet()) {
            proc.getConflicts().clear();
        } // FOR
        for (Procedure proc : this.procedures.keySet()) {
            ProcedureInfo pInfo = this.procedures.get(proc);
            if (pInfo.rwConflicts.isEmpty() && pInfo.wwConflicts.isEmpty()) {
                continue;
            }
            
            this.updateCatalog(pInfo, pInfo.rwConflicts, true);
            this.updateCatalog(pInfo, pInfo.wwConflicts, false);
            
            if (debug.get())
                LOG.debug(String.format(
                          "%s Conflicts:\n" +
                          "  R-W: %s\n" +
                          "  W-W: %s",
                          proc.getName(), pInfo.rwConflicts.keySet(), pInfo.wwConflicts));
        } // FOR
    }
    
    private void updateCatalog(ProcedureInfo pInfo, Map<Procedure, Collection<Table>> conflicts, final boolean readWrite) {
        for (Procedure conflict_proc : conflicts.keySet()) {
            ConflictSet cSet = pInfo.proc.getConflicts().get(conflict_proc.getName());
            if (cSet == null) {
                cSet = pInfo.proc.getConflicts().add(conflict_proc.getName());
            }
            for (Table catalog_tbl : conflicts.get(conflict_proc)) {
                TableRef ref = null;
                if (readWrite) {
                    ref = cSet.getReadwriteconflicts().add(catalog_tbl.getName());
                } else {
                    ref = cSet.getWritewriteconflicts().add(catalog_tbl.getName());
                }
                ref.setTable(catalog_tbl);
            } // FOR
        } // FOR
    }
    
    
    /**
     * Calculate whether two Procedures conflict
     * @param proc0
     * @param proc1
     * @return
     * @throws Exception
     */
    protected Collection<Table> checkReadWriteConflict(Procedure proc0, Procedure proc1) throws Exception {
        ProcedureInfo pInfo0 = this.procedures.get(proc0);
        assert(pInfo0 != null);
        ProcedureInfo pInfo1 = this.procedures.get(proc1);
        assert(pInfo1 != null);
        
        Set<Table> conflicts = new HashSet<Table>();
        
        Collection<Column> cols0 = new HashSet<Column>();
        
        // Read-Write Conflicts
        for (Statement stmt0 : pInfo0.readQueries) {
            if (this.ignoredStatements.contains(stmt0)) continue;
            
            Collection<Table> tables0 = CatalogUtil.getReferencedTables(stmt0);
            cols0.clear();
            cols0.addAll(CatalogUtil.getReferencedColumns(stmt0));
            cols0.addAll(PlanNodeUtil.getOutputColumnsForStatement(stmt0));
            assert(cols0.isEmpty() == false) : "No columns for " + stmt0.fullName();
            
            for (Statement stmt1 : pInfo1.writeQueries) {
                if (this.ignoredStatements.contains(stmt1)) continue;
                
                Collection<Table> tables1 = CatalogUtil.getReferencedTables(stmt1);
                Collection<Table> intersectTables = CollectionUtils.intersection(tables0, tables1);
                if (debug.get()) LOG.debug(String.format("RW %s <-> %s - Intersection Tables %s",
                                           stmt0.fullName(), stmt1.fullName(), intersectTables));
                if (intersectTables.isEmpty()) continue;
                
                // Ok let's be crafty here...
                // The two queries won't conflict if the write query is an UPDATE
                // and it changes a column that is not referenced in either 
                // the output or the where clause for the SELECT query
                if (stmt1.getQuerytype() == QueryType.UPDATE.getValue()) {
                    Collection<Column> cols1 = CatalogUtil.getModifiedColumns(stmt1);
                    assert(cols1.isEmpty() == false) : "No columns for " + stmt1.fullName();
                    Collection<Column> intersectColumns = CollectionUtils.intersection(cols0, cols1);
                    if (debug.get()) LOG.debug(String.format("RW %s <-> %s - Intersection Columns %s",
                                               stmt0.fullName(), stmt1.fullName(), intersectColumns));
                    if (intersectColumns.isEmpty()) continue;
                }
                
                conflicts.addAll(intersectTables);
                if (debug.get()) LOG.debug(String.format("RW %s <-> %s CONFLICTS",
                                           stmt0.fullName(), stmt1.fullName()));
            } // FOR (proc1)
        } // FOR (proc0)
        
        return (conflicts);
    }
    
    /**
     * Calculate whether two Procedures conflict
     * @param proc0
     * @param proc1
     * @return
     * @throws Exception
     */
    protected Collection<Table> checkWriteWriteConflict(Procedure proc0, Procedure proc1) throws Exception {
        ProcedureInfo pInfo0 = this.procedures.get(proc0);
        assert(pInfo0 != null);
        ProcedureInfo pInfo1 = this.procedures.get(proc1);
        assert(pInfo1 != null);
        
        Set<Table> conflicts = new HashSet<Table>();
        
        // Write-Write Conflicts
        // Any INSERT or DELETE is always a conflict
        // For UPDATE, we will check whether their columns intersect
        for (Statement stmt0 : pInfo0.writeQueries) {
            if (this.ignoredStatements.contains(stmt0)) continue;
            Collection<Table> tables0 = CatalogUtil.getReferencedTables(stmt0);
            QueryType type0 = QueryType.get(stmt0.getQuerytype());
            Collection<Column> cols0 = CatalogUtil.getReferencedColumns(stmt0);
            
            // If there are no columns, then this must be a delete
//            assert(cols0.isEmpty() == false) : "No columns for " + stmt0.fullName();
            
            for (Statement stmt1 : pInfo1.writeQueries) {
                if (this.ignoredStatements.contains(stmt1)) continue;
                Collection<Table> tables1 = CatalogUtil.getReferencedTables(stmt1);
                QueryType type1 = QueryType.get(stmt1.getQuerytype());
                
                Collection<Table> intersectTables = CollectionUtils.intersection(tables0, tables1);
                if (debug.get()) LOG.debug(String.format("WW %s <-> %s - Intersection Tables %s",
                                           stmt0.fullName(), stmt1.fullName(), intersectTables));
                if (intersectTables.isEmpty()) continue;
                
                if (type0 == QueryType.INSERT || type1 == QueryType.INSERT ||
                    type0 == QueryType.DELETE || type1 == QueryType.DELETE) {
                    conflicts.addAll(intersectTables);
                    break;
                }
                
                Collection<Column> cols1 = CatalogUtil.getReferencedColumns(stmt1);
                assert(cols1.isEmpty() == false) : "No columns for " + stmt0.fullName();
                Collection<Column> intersectColumns = CollectionUtils.intersection(cols0, cols1);
                if (debug.get()) LOG.debug(String.format("WW %s <-> %s - Intersection Columns %s",
                                           stmt0.fullName(), stmt1.fullName(), intersectColumns));
                if (intersectColumns.isEmpty()) continue;
                
                conflicts.addAll(intersectTables);
            } // FOR (proc1)
        } // FOR (proc0)
        return (conflicts);
    }
}
