package edu.brown.hstore.specexec;

import java.util.Collection;

import org.voltdb.CatalogContext;
import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.Procedure;

import edu.brown.catalog.conflicts.ConflictSetUtil;
import edu.brown.hstore.txns.AbstractTransaction;
import edu.brown.hstore.txns.LocalTransaction;
import edu.brown.mappings.ParameterMappingsSet;

/**
 * A more fine-grained ConflictChecker based on estimations of what 
 * rows the txns will read/write.
 * @author pavlo
 */
public class RowConflictChecker extends AbstractConflictChecker {

    private final ParameterMappingsSet paramMappings;
    private final boolean disabled;
    
    public RowConflictChecker(CatalogContext catalogContext) {
        super(catalogContext);
        this.paramMappings = catalogContext.paramMappings;
        this.disabled = (this.paramMappings == null);
    }
    
    private void precomputeConflicts(Procedure proc0) {
        // For each Procedure (including the one we're calling this method for), we 
        // want to compute the minimum number of ConflictParameterPairs that we need
        // to evaluate in order to determine that two transactions will never be
        // conflicting.
        for (Procedure proc1 : this.catalogContext.getRegularProcedures()) {
             Collection<ConflictPair> conflictPairs = ConflictSetUtil.getAllConflictPairs(proc0, proc1);
             
             
        }
    }

    @Override
    public boolean ignoreProcedure(Procedure proc) {
        return (this.disabled);
    }

    @Override
    public boolean isConflicting(AbstractTransaction dtxn, LocalTransaction ts, int partitionId) {
        // TODO Auto-generated method stub
        return false;
    }

}
