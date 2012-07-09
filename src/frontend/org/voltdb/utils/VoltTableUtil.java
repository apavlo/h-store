package org.voltdb.utils;

import java.util.Arrays;
import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.types.SortDirectionType;

import edu.brown.utils.StringUtil;

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
    

    /**
     * Returns a row with random data that can be added to this VoltTable
     * @param table
     * @return
     */
    public static Object[] getRandomRow(Table catalog_tbl) {
        Object row[] = new Object[catalog_tbl.getColumns().size()];
        for (Column catalog_col : catalog_tbl.getColumns()) {
            int i = catalog_col.getIndex();
            VoltType vtype = VoltType.get(catalog_col.getType());
            row[i] = VoltTypeUtil.getRandomValue(vtype);
            if (vtype == VoltType.STRING) {
                row[i] = StringUtil.abbrv(row[i].toString(), catalog_col.getSize(), false);
            }
        } // FOR
        return (row);
    }
    
}
