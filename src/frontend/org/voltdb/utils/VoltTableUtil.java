package org.voltdb.utils;

import java.util.Arrays;

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
    
}
