package org.voltdb.utils;

import java.util.Arrays;
import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.types.SortDirectionType;

public abstract class VoltTableUtil {

    public static VoltTable sort(VoltTable table, Pair<Integer, SortDirectionType>...cols) {
        if (cols.length == 0) return (table);
        
        Object rows[][] = new Object[table.getRowCount()][];
        table.resetRowPosition();
        int row_idx = -1;
        while (table.advanceRow()) {
            rows[++row_idx] = table.getRowArray();
            assert(rows[row_idx] != null) : "Null row at " + row_idx;
        } // FOR
        
        VoltTableComparator comparator = new VoltTableComparator(table, cols);
        Arrays.sort(rows, comparator);
        
        VoltTable clone = new VoltTable(table);
        for (int i = 0; i < rows.length; i++) {
            clone.addRow(rows[i]);
        } // FOR
        
        return (clone);
    }
    
    /**
     * Combine multiple VoltTables into a single object
     * This assumes that all of the tables have the same schema
     * @param tables
     * @return
     */
    public static VoltTable combine(Collection<VoltTable> tables) {
        VoltTable result = null;
        for (VoltTable vt : tables) {
            if (vt == null) continue;
            if (result == null) {
                result = new VoltTable(vt);
            }
            vt.resetRowPosition();
            while (vt.advanceRow()) {
                result.add(vt);
            } // WHILE
        } // FOR
        return (result);
    }
    
}
