package org.voltdb.utils;

import java.io.Writer;
import java.util.Arrays;
import java.util.Collection;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.types.SortDirectionType;

import au.com.bytecode.opencsv.CSVWriter;
import edu.brown.utils.StringBoxUtil;
import edu.brown.utils.StringUtil;
import edu.brown.utils.TableUtil;

public abstract class VoltTableUtil {

    public static VoltTable.ColumnInfo[] extractColumnInfo(VoltTable vt) {
        VoltTable.ColumnInfo cols[] = new VoltTable.ColumnInfo[vt.getColumnCount()];
        for (int i = 0; i < cols.length; i++) {
            cols[i] = new VoltTable.ColumnInfo(vt.getColumnName(i), vt.getColumnType(i));
        } // FOR
        return (cols);
    }
    
    /**
     * Dump out a VoltTable as a CSV to the given writer
     * If the header flag is set to true, then the output will include 
     * the column names in the first row
     * @param out
     * @param vt
     * @param write_header
     */
    public static void csv(Writer out, VoltTable vt, boolean header) {
        CSVWriter writer = new CSVWriter(out);
        
        if (header) {
            String cols[] = new String[vt.getColumnCount()];
            for (int i = 0; i < cols.length; i++) {
                cols[i] = vt.getColumnName(i);
            } // FOR
            writer.writeNext(cols);
        }
        vt.resetRowPosition();
        while (vt.advanceRow()) {
            String row[] = vt.getRowStringArray();
            assert(row != null);
            assert(row.length == vt.getColumnCount());
            writer.writeNext(row);
        } // WHILE
    }
    
    /**
     * Pretty-printer for an array of VoltTables. This will reset
     * each VoltTable's row position both before and after generating the
     * formatted output
     * @param results
     * @return
     */
    public static String format(VoltTable...results) {
        StringBuilder sb = new StringBuilder();
        final int num_results = results.length;
        
        TableUtil.Format f = TableUtil.defaultTableFormat();
        f.spacing_col = true;
        f.trim_all = true;
        f.delimiter_all = " | ";

        // TABLE RESULTS
        for (int result_idx = 0; result_idx < num_results; result_idx++) {
            if (result_idx > 0) sb.append("\n\n");
            
            VoltTable vt = results[result_idx];
            vt.resetRowPosition();
            String header[] = new String[vt.getColumnCount()];
            for (int i = 0; i < header.length; i++) {
                String colName = vt.getColumnName(i);
                header[i] = (colName.isEmpty() ? "<empty>" : colName);
            } // FOR
            
            Object rows[][] = new Object[vt.getRowCount()][];
            f.delimiter_rows = new String[rows.length];
            for (int i = 0; i < rows.length; i++) {
                rows[i] = new Object[header.length];
                f.delimiter_rows[i] = "-";
                
                boolean adv = vt.advanceRow();
                assert(adv);
                for (int j = 0; j < header.length; j++) {
                    rows[i][j] = vt.get(j);
                    if (vt.wasNull()) {
                        rows[i][j] = null;
                    }
                } // FOR (cols)
                
            } // FOR (rows)
            
            sb.append(String.format("Result #%d / %d\n", result_idx+1, num_results));
            
            String resultTable = TableUtil.table(f, header, rows);
            resultTable = StringBoxUtil.box(resultTable,
                                         StringBoxUtil.UNICODE_BOX_HORIZONTAL,
                                         StringBoxUtil.UNICODE_BOX_VERTICAL,
                                         null,
                                         StringBoxUtil.UNICODE_BOX_CORNERS);
            sb.append(StringUtil.prefix(resultTable, "  "));
            vt.resetRowPosition();
        } // FOR
        return (sb.toString());
    }

    /**
     * Sort a VoltTable based on a series of columns and directions.
     * Note that this utility method is not a sort in place and thus it 
     * will create a second copy of the table's data. 
     * @param table
     * @param cols
     * @return
     */
    @SuppressWarnings("unchecked")
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
    public static VoltTable union(Collection<VoltTable> tables) {
        VoltTable result = null;
        if (tables != null) {
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
        }
        return (result);
    }
    
    
    /**
     * Returns a row with random data that can be added to this VoltTable
     * @param table
     * @return
     */
    public static Object[] getRandomRow(VoltTable volt_tbl) {
        Object row[] = new Object[volt_tbl.getColumnCount()];
        for (int i = 0; i < row.length; i++) {
            VoltType vtype = volt_tbl.getColumnType(i);
            row[i] = VoltTypeUtil.getRandomValue(vtype);
            // HACK: We don't actually now the VARCHAR length here, 
            // so we'll just leave it at 8
            if (vtype == VoltType.STRING) {
                row[i] = StringUtil.abbrv(row[i].toString(), 8, false);
            }
        } // FOR
        return (row);
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
