package edu.brown.catalog;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;
import org.voltdb.catalog.Statement;
import org.voltdb.types.QueryType;

import edu.brown.logging.LoggerUtil;
import edu.brown.logging.LoggerUtil.LoggerBoolean;

public class ConflictCalculator {
    private static final Logger LOG = Logger.getLogger(ConflictCalculator.class);
    private static final LoggerBoolean debug = new LoggerBoolean(LOG.isDebugEnabled());
    private static final LoggerBoolean trace = new LoggerBoolean(LOG.isTraceEnabled());
    static {
        LoggerUtil.attachObserver(LOG, debug, trace);
    }
    
    private class ProcedureInfo {
        final Set<Statement> readQueries = new HashSet<Statement>();
        final Set<Statement> writeQueries = new HashSet<Statement>();
        final Set<Procedure> conflicts = new HashSet<Procedure>();
    }
    
    private final Database catalog_db;
    private final Map<Procedure, ProcedureInfo> procedures = new HashMap<Procedure, ConflictCalculator.ProcedureInfo>(); 
    
    public ConflictCalculator(Catalog catalog) {
        this.catalog_db = CatalogUtil.getDatabase(catalog);

        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            if (catalog_proc.getMapreduce() || catalog_proc.getSystemproc()) continue;
            
            ProcedureInfo pInfo = new ProcedureInfo();
            for (Statement catalog_stmt : catalog_proc.getStatements()) {
                if (catalog_stmt.getQuerytype() == QueryType.SELECT.getValue()) {
                    pInfo.readQueries.add(catalog_stmt);
                } else {
                    pInfo.writeQueries.add(catalog_stmt);
                }
            } // FOR (statement)
            this.procedures.put(catalog_proc, pInfo);
        } // FOR (procedure)
    }
    
    public void process() throws Exception {
        // For each Procedure, get the list of read and write queries
        // Then check whether there is a R-W or W-R conflict with other procedures
        for (Procedure catalog_proc : this.procedures.keySet()) {
            ProcedureInfo pInfo = this.procedures.get(catalog_proc);
            assert(pInfo != null);
        } // FOR

    }
    
    public void updateCatalog() {
        
    }
    
    
}
