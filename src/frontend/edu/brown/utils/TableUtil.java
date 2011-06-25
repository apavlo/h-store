package edu.brown.utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;

public abstract class TableUtil {

    private static Pattern LINE_SPLIT = Pattern.compile("\n");
    
    public static final String CSV_DELIMITER = ",";
    public static final String TABLE_DELIMITER = "  ";
    
    public static class Format {
        public final String delimiter_all;
        public final String delimiter_cols[];
        public final String delimiter_rows[];
        public final boolean spacing;
        public final boolean spacing_all;
        public final boolean right_align;
        public final boolean quote_rows;
        public final boolean quote_header;
        public final boolean trim_all;
        public final boolean capitalize_header;
        
        /**
         * Create a new TableUtil format object
         * @param delimiter String used in between columns
         * @param delimiter_rows TODO
         * @param spacing Whether the cells in a single columns will all be the same width
         * @param spacing_all Whether all the cells in the entire table will be the same width
         * @param right_align Whether to right align each cell
         * @param quote_rows Whether to wrap each non-header cell in quotes (including spacing)
         * @param quote_header Whether to wrap ach header cell in quotes (including spacing)
         * @param trim_all TODO
         * @param capitalize_header TODO
         */
        public Format(String delimiter,
                      String delimiter_cols[],
                      String[] delimiter_rows,
                      boolean spacing,
                      boolean spacing_all,
                      boolean right_align,
                      boolean quote_rows,
                      boolean quote_header,
                      boolean trim_all,
                      boolean capitalize_header) {
            this.delimiter_all = delimiter;
            this.delimiter_cols = delimiter_cols;
            this.delimiter_rows = delimiter_rows;
            this.spacing = spacing;
            this.spacing_all = spacing_all;
            this.right_align = right_align;
            this.quote_rows = quote_rows;
            this.quote_header = quote_header;
            this.trim_all = trim_all;
            this.capitalize_header = capitalize_header;
        }
        
        @Override
        public String toString() {
            Class<?> confClass = this.getClass();
            StringBuilder sb = new StringBuilder();
            for (Field f : confClass.getFields()) {
                Object obj = null;
                try {
                    obj = f.get(this);
                    if (ClassUtil.isArray(obj)) {
                        obj = Arrays.toString((Object[])obj);
                    }
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
                if (sb.length() > 0) sb.append(", ");
                sb.append(String.format("%s=%s", f.getName(), obj));
            } // FOR
            return sb.toString();
        }
    }
    
    private static Format DEFAULT_TABLE = null;
    private static Format DEFAULT_CSV = null;
    
    public static Format defaultTableFormat() {
        if (DEFAULT_TABLE == null) {
            DEFAULT_TABLE = new Format(TABLE_DELIMITER, null, null, true, false, false, false, false, false, false);
        }
        return (DEFAULT_TABLE);
    }
    
    public static Format defaultCSVFormat() {
        if (DEFAULT_CSV == null) {
            DEFAULT_CSV = new Format(CSV_DELIMITER, null, null, false, false, false, false, false, false, false);
        }
        return (DEFAULT_CSV);
    }
    
    /**
     * Format the header + rows into a CSV
     * @param header
     * @param rows
     * @return
     */
    public static String csv(String header[], Object[]...rows) {
        return (table(defaultCSVFormat(), header, rows));        
    }
    
    /**
     * Format the header + rows into a neat little table
     * @param header
     * @param rows
     * @return
     */
    public static String table(String header[], Object[]...rows) {
        return (table(defaultTableFormat(), header, rows));
    }
    
    /**
     * 
     * @param header
     * @param rows
     * @return
     */
    public static Map<String, String> tableMap(String header[], Object[]...rows) {
        return (tableMap(defaultTableFormat(), header, rows));
    }
    
    /**
     * 
     * @param spacing
     * @param spacing_all TODO
     * @param right_align TODO
     * @param header
     * @param rows
     * @return
     */
    public static Map<String, String> tableMap(Format format, String header[], Object[]...rows) {
        Object key_cols[] = new String[rows.length+1];
        
        String new_header[] = new String[header.length-1];
        for (int i = 0; i < header.length; i++) {
            if (i == 0) {
                key_cols[0] = header[i];
            } else {
                new_header[i-1] = header[i];
            }
        } // FOR
        Object new_rows[][] = new String[rows.length][];
        for (int i = 0; i < new_rows.length; i++) {
            Object row[] = rows[i];
            assert(row != null) : "Null row at " + i;
            key_cols[i+1] = row[0];
            
            new_rows[i] = new String[row.length - 1];
            for (int j = 0; j < new_rows[i].length; j++) {
                new_rows[i][j] = (row[j+1] != null ? row[j+1].toString() : "");
            } // FOR
        } // FOR
        
        Map<String, String> m = new ListOrderedMap<String, String>();
        String lines[] = LINE_SPLIT.split(table(format, new_header, new_rows));
        int row_idx = 0; 
        Integer key_width = null;
        boolean last_row_was_delimiter = false;
        for (int line_idx = 0; line_idx < lines.length; line_idx++) {
            Object key = null;
            
            // ROW DELIMITER
            if (row_idx > 0 && last_row_was_delimiter == false && format.delimiter_rows != null && format.delimiter_rows[row_idx-1] != null) {
                if (key_width == null) {
                    key_width = 0;
                    for (Object k : key_cols) {
                        if (k != null) key_width = Math.max(key_width, k.toString().length());
                    } // FOR
                }
//                key = StringUtil.repeat(format.delimiter_rows[row_idx-1], key_width);
                key = StringUtil.repeat(" ", row_idx);
                last_row_was_delimiter = true;
                
            // FIRST COLUMN
            } else {
                key = key_cols[row_idx++];
                last_row_was_delimiter = false;
            }
            m.put((key != null ? key.toString() : null), lines[line_idx]);
        } // FOR
        return (m);
        
    }
        
    /**
     * Format the header+rows into a table
     * @param delimiter_all the character to use in between cells
     * @param spacing whether to make the width of each column the size of the largest cell
     * @param spacing_all TODO
     * @param right_align TODO
     * @param quote_rows whether to surround each cell in quotation marks
     * @param header
     * @param rows
     * @return
     */
    public static String table(Format format, String header[], Object[]...rows) {
        // First we need to figure out the size for each column
        String col_formats[] = new String[header.length];
        String header_formats[] = new String[header.length];        
        Integer max_width = 0;
        int total_width = 0;
        Integer widths[] = new Integer[header.length];
        for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
            Integer width = (header[col_idx] != null ? header[col_idx].length() : 0);
            for (int row_idx = 0; row_idx < rows.length; row_idx++) {
                String val = "";
                if (rows[row_idx][col_idx] != null) {
                    val = rows[row_idx][col_idx].toString();
                    if (format.trim_all) val = val.trim();
                }
                width = Math.max(width, val.length());
            } // FOR
            widths[col_idx] = width;
            total_width += width;
            if (format.spacing_all) max_width = Math.max(width, max_width);
        }  // FOR
        for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
            // FORMAT
            Integer width = (format.spacing_all ? max_width : widths[col_idx]);
            final String f = "%" + 
                             (width != null ? (format.right_align ? "" : "-") + width : "") + 
                             "s";
            
            // DELIMITER
            String d = "";
            if (format.delimiter_cols != null && format.delimiter_cols[col_idx] != null) {
                d = format.delimiter_cols[col_idx];
            } else if (col_idx > 0) {
                d = format.delimiter_all;
            }
            total_width += d.length();
            
            // COLUMN
            String col_f = f;
            if (format.quote_rows) col_f = '"' + col_f + '"';
            col_formats[col_idx] = d + col_f;
            
            // HEADER
            String header_f = f;
            if (format.quote_header) header_f = '"' + header_f + '"';
            header_formats[col_idx] = d + header_f;
        } // FOR
        
        // Create header row
        StringBuilder sb = new StringBuilder();
        for (int col_idx = 0; col_idx < header_formats.length; col_idx++) {
            String cell = header[col_idx];
            if (format.capitalize_header) cell = cell.toUpperCase();
            sb.append(String.format(header_formats[col_idx], cell));
        } // FOR
        
        // Now dump out the table
        for (int row_idx = 0; row_idx < rows.length; row_idx++) {
            Object row[] = rows[row_idx];
            sb.append("\n");
            
            // Add row delimiter if necessary
            if (format.delimiter_rows != null && format.delimiter_rows[row_idx] != null) {
                sb.append(StringUtil.repeat(format.delimiter_rows[row_idx], total_width)).append("\n");
            }
            
            for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
                String cell = (row[col_idx] != null ? row[col_idx].toString() : "");
                if (format.quote_rows && (row_idx > 0 || format.quote_header)) cell = '"' + cell + '"';
                sb.append(String.format(col_formats[col_idx], cell));    
            } // FOR
        } // FOR
        
        return (sb.toString());
    }
    
}
