/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  http://hstore.cs.brown.edu/                                            *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/
package edu.brown.utils;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.collections15.map.ListOrderedMap;
import org.apache.log4j.Logger;

public abstract class TableUtil {
    private static final Logger LOG = Logger.getLogger(TableUtil.class);

    private static Pattern LINE_SPLIT = Pattern.compile("\n");

    public static final String CSV_DELIMITER = ",";
    public static final String TABLE_DELIMITER = "  ";

    public static class Format {
        public String delimiter_all;
        public String delimiter_cols[];
        public String delimiter_rows[];
        public boolean spacing_col;
        public boolean spacing_all;
        public boolean right_align;
        public boolean quote_rows;
        public boolean quote_header;
        public boolean trim_all;
        public boolean capitalize_header;
        public boolean prune_null_rows;
        public Object replace_null_cells;

        private Format() {
            // Nothing
        }

        /**
         * Create a new TableUtil format object
         * 
         * @param delimiter_all
         *            String used in between all columns
         * @param delimiter_cols
         *            TODO
         * @param delimiter_rows
         *            TODO
         * @param spacing_col
         *            Whether the cells in a single columns will all be the same
         *            width
         * @param spacing_all
         *            Whether all the cells in the entire table will be the same
         *            width
         * @param right_align
         *            Whether to right align each cell
         * @param quote_rows
         *            Whether to wrap each non-header cell in quotes (including
         *            spacing)
         * @param quote_header
         *            Whether to wrap each header cell in quotes (including
         *            spacing)
         * @param trim_all
         *            Whether to invoke String.trim() on each cell
         * @param capitalize_header
         *            TODO
         * @param prune_null_rows
         *            If true, then any null row will be skipped
         * @param replace_null_cells
         *            TODO
         */
        public Format(String delimiter_all, String delimiter_cols[], String[] delimiter_rows, boolean spacing_col, boolean spacing_all, boolean right_align, boolean quote_rows, boolean quote_header,
                boolean trim_all, boolean capitalize_header, boolean prune_null_rows, Object replace_null_cells) {
            this.delimiter_all = delimiter_all;
            this.delimiter_cols = delimiter_cols;
            this.delimiter_rows = delimiter_rows;
            this.spacing_col = spacing_col;
            this.spacing_all = spacing_all;
            this.right_align = right_align;
            this.quote_rows = quote_rows;
            this.quote_header = quote_header;
            this.trim_all = trim_all;
            this.capitalize_header = capitalize_header;
            this.prune_null_rows = prune_null_rows;
            this.replace_null_cells = replace_null_cells;
        }

        public Format clone() {
            Format clone = new Format();
            clone.delimiter_all = this.delimiter_all;
            clone.delimiter_cols = this.delimiter_cols;
            clone.delimiter_rows = this.delimiter_rows;
            clone.spacing_col = this.spacing_col;
            clone.spacing_all = this.spacing_all;
            clone.right_align = this.right_align;
            clone.quote_rows = this.quote_rows;
            clone.quote_header = this.quote_header;
            clone.trim_all = this.trim_all;
            clone.capitalize_header = this.capitalize_header;
            clone.prune_null_rows = this.prune_null_rows;
            clone.replace_null_cells = this.replace_null_cells;
            return (clone);
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
                        obj = Arrays.toString((Object[]) obj);
                    }
                } catch (IllegalAccessException ex) {
                    throw new RuntimeException(ex);
                }
                if (sb.length() > 0)
                    sb.append(", ");
                sb.append(String.format("%s=%s", f.getName(), obj));
            } // FOR
            return sb.toString();
        }
    }

    private static Format DEFAULT_TABLE = null;
    private static Format DEFAULT_CSV = null;

    public static Format defaultTableFormat() {
        if (DEFAULT_TABLE == null) {
            DEFAULT_TABLE = new Format(TABLE_DELIMITER, null, null, true, false, false, false, false, false, false, false, null);
        }
        return (DEFAULT_TABLE);
    }

    public static Format defaultCSVFormat() {
        if (DEFAULT_CSV == null) {
            DEFAULT_CSV = new Format(CSV_DELIMITER, null, null, false, false, false, false, false, false, false, true, null);
        }
        return (DEFAULT_CSV);
    }

    /**
     * Format the header + rows into a CSV
     * 
     * @param header
     * @param rows
     * @return
     */
    public static String csv(String header[], Object[]... rows) {
        return (table(defaultCSVFormat(), header, rows));
    }

    /**
     * Format the header + rows into a neat little table
     * 
     * @param header
     * @param rows
     * @return
     */
    public static String table(String header[], Object[]... rows) {
        return (table(defaultTableFormat(), header, rows));
    }

    /**
     * Format the rows into a neat little table
     * 
     * @param rows
     * @return
     */
    public static String table(Object[]... rows) {
        return (table(defaultTableFormat(), null, rows));
    }

    /**
     * @param header
     * @param rows
     * @return
     */
    public static Map<String, String> tableMap(String header[], Object[]... rows) {
        return (tableMap(defaultTableFormat(), header, rows));
    }

    /**
     * @param spacing_col
     * @param spacing_all
     *            TODO
     * @param right_align
     *            TODO
     * @param header
     * @param rows
     * @return
     */
    public static Map<String, String> tableMap(Format format, String header[], Object[]... rows) {
        int num_rows = rows.length;
        if (format.prune_null_rows) {
            for (int i = 0; i < num_rows; i++) {
                if (rows[i] == null)
                    num_rows--;
            } // FOR
        }

        Object key_cols[] = new String[num_rows + 1];
        String new_header[] = new String[header.length - 1];
        for (int i = 0; i < header.length; i++) {
            if (i == 0) {
                key_cols[0] = header[i];
            } else {
                new_header[i - 1] = header[i];
            }
        } // FOR
        Object new_rows[][] = new String[num_rows][];
        int new_row_idx = 0;
        for (int i = 0; i < rows.length; i++) {
            Object orig_row[] = rows[i];
            if (orig_row == null && format.prune_null_rows)
                continue;
            assert (orig_row != null) : "Null row at " + i;
            key_cols[new_row_idx + 1] = orig_row[0];

            new_rows[new_row_idx] = new String[orig_row.length - 1];
            for (int j = 0; j < new_rows[i].length; j++) {
                new_rows[new_row_idx][j] = (orig_row[j + 1] != null ? orig_row[j + 1].toString() : null);
            } // FOR
            new_row_idx++;
        } // FOR

        Map<String, String> m = new ListOrderedMap<String, String>();
        String lines[] = LINE_SPLIT.split(table(format, new_header, new_rows));
        int row_idx = 0;
        Integer key_width = null;
        boolean last_row_was_delimiter = false;
        for (int line_idx = 0; line_idx < lines.length; line_idx++) {
            Object key = null;

            // ROW DELIMITER
            if (row_idx > 0 && last_row_was_delimiter == false && format.delimiter_rows != null && format.delimiter_rows[row_idx - 1] != null) {
                if (key_width == null) {
                    key_width = 0;
                    for (Object k : key_cols) {
                        if (k != null)
                            key_width = Math.max(key_width, k.toString().length());
                    } // FOR
                }
                // key = StringUtil.repeat(format.delimiter_rows[row_idx-1],
                // key_width);
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
     * 
     * @param delimiter_all
     *            the character to use in between cells
     * @param spacing_col
     *            whether to make the width of each column the size of the
     *            largest cell
     * @param spacing_all
     *            TODO
     * @param right_align
     *            TODO
     * @param quote_rows
     *            whether to surround each cell in quotation marks
     * @param header
     * @param rows
     * @return
     */
    public static String table(final Format format, final String header[], final Object[]... rows) {
        final boolean debug = LOG.isDebugEnabled();

        String replace_null_str = (format.replace_null_cells != null ? format.replace_null_cells.toString() : null);
        if (debug) {
            LOG.debug("format.replace_null_cells = " + format.replace_null_cells);
            LOG.debug("replace_null_str = " + replace_null_str);
        }

        // First we need to figure out the size for each column
        int num_cols = 0;
        if (header != null) {
            num_cols = header.length;
        } else {
            for (int row_idx = 0; row_idx < rows.length; row_idx++) {
                if (rows[row_idx] != null)
                    num_cols = Math.max(num_cols, rows[row_idx].length);
            } // FOR
        }

        // Internal data structures about the table
        String col_formats[] = new String[num_cols];
        String header_formats[] = new String[num_cols];
        Integer max_width = 0;
        int total_width = 0;
        Integer widths[] = new Integer[num_cols];
        String row_strs[][] = new String[rows.length][num_cols];

        for (int col_idx = 0; col_idx < num_cols; col_idx++) {
            Integer width = (header != null && header[col_idx] != null ? header[col_idx].length() : 0);
            for (int row_idx = 0; row_idx < rows.length; row_idx++) {
                if (rows[row_idx] == null)
                    continue;
                if (col_idx == 0)
                    row_strs[row_idx] = new String[num_cols];
                String val = replace_null_str;
                if (col_idx < rows[row_idx].length) {
                    val = (rows[row_idx][col_idx] != null ? rows[row_idx][col_idx].toString() : replace_null_str);
                    if (val != null) {
                        if (format.trim_all)
                            val = val.trim();
                        width = Math.max(width, val.length());
                    }
                }
                row_strs[row_idx][col_idx] = val;
                if (debug)
                    LOG.debug(String.format("[%d, %d] = %s", row_idx, col_idx, row_strs[row_idx][col_idx]));
            } // FOR
            widths[col_idx] = width;
            total_width += width;
            if (format.spacing_all)
                max_width = Math.max(width, max_width);
        } // FOR
        for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
            // FORMAT
            Integer width = (format.spacing_all ? max_width : (format.spacing_col ? widths[col_idx] : null));
            final String f = "%" + (width != null ? (format.right_align ? "" : "-") + width : "") + "s";

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
            if (format.quote_rows)
                col_f = '"' + col_f + '"';
            col_formats[col_idx] = d + col_f;

            // HEADER
            if (header != null) {
                String header_f = f;
                if (format.quote_header)
                    header_f = '"' + header_f + '"';
                header_formats[col_idx] = d + header_f;
            }
        } // FOR

        // Create header row
        StringBuilder sb = new StringBuilder();
        if (header != null) {
            for (int col_idx = 0; col_idx < header_formats.length; col_idx++) {
                String cell = header[col_idx];
                if (format.capitalize_header && cell != null)
                    cell = cell.toUpperCase();
                sb.append(String.format(header_formats[col_idx], cell));
            } // FOR
        }

        // Now dump out the table
        for (int row_idx = 0; row_idx < rows.length; row_idx++) {
            Object row[] = rows[row_idx];
            if (format.prune_null_rows && row == null)
                continue;
            if (row_idx > 0 || (row_idx == 0 && header != null))
                sb.append("\n");

            // Add row delimiter if necessary
            if (format.delimiter_rows != null && row_idx < format.delimiter_rows.length && format.delimiter_rows[row_idx] != null) {
                sb.append(StringUtil.repeat(format.delimiter_rows[row_idx], total_width)).append("\n");
            }

            for (int col_idx = 0; col_idx < col_formats.length; col_idx++) {
                String cell = row_strs[row_idx][col_idx];
                if (format.quote_rows && (row_idx > 0 || format.quote_header))
                    cell = '"' + cell + '"';
                sb.append(String.format(col_formats[col_idx], cell));
            } // FOR
        } // FOR

        return (sb.toString());
    }

}
