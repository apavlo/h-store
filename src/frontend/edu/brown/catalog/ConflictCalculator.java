package edu.brown.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Catalog;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Procedure;

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
        
    }
    
    private final Database catalog_db;
    private final Map<Procedure, Set<Procedure>> procConflicts = new HashMap<Procedure, Set<Procedure>>(); 
    
    public ConflictCalculator(Catalog catalog) {
        this.catalog_db = CatalogUtil.getDatabase(catalog);
    }
    
    public void process() throws Exception {
        
        // For each Procedure, get the list of read and write queries
        // Then check whether there is a R-W or W-R conflict with other procedures 
        for (Procedure catalog_proc : catalog_db.getProcedures()) {
            
            
        } // FOR
        
        
        
    }
    
    public void updateCatalog() {
        
    }
    
    
}
