package edu.brown.catalog.conflicts;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.voltdb.catalog.ConflictPair;
import org.voltdb.catalog.Table;

import edu.brown.catalog.CatalogUtil;

public abstract class ConflictSetUtil {

    /**
     * Return the set of all Tables referenced in the collection of ConflictPairs
     * @param conflicts
     * @return
     */
    public static Collection<Table> getAllTables(Collection<ConflictPair> conflicts) {
        Set<Table> tables = new HashSet<Table>();
        for (ConflictPair cp : conflicts) {
            tables.addAll(CatalogUtil.getTablesFromRefs(cp.getTables()));
        } // FOR
        return (tables);
    }
    
}
