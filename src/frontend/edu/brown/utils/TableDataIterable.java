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
/**
 * 
 */
package edu.brown.utils;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.Table;
import org.voltdb.utils.CatalogUtil;
import org.voltdb.utils.VoltTypeUtil;

import au.com.bytecode.opencsv.CSVReader;

/**
 * @author pavlo
 */
public class TableDataIterable implements Iterable<Object[]> {
    private static final Logger LOG = Logger.getLogger(TableDataIterable.class.getName());

    private final Table catalog_tbl;
    private final File table_file;
    private final CSVReader reader;
    private final VoltType types[];
    private final boolean fkeys[];
    private final boolean nullable[];
    private final boolean auto_generate_first_column;

    private final DateFormat timestamp_formats[] = new DateFormat[] { new SimpleDateFormat("yyyy-MM-dd"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"), };
    private Set<Column> truncate_warnings = new HashSet<Column>();
    private int line_ctr = 0;

    /**
     * Constructor
     * 
     * @param catalog_tbl
     * @param table_file
     * @param has_header
     *            whether we expect the data file to include a header in the
     *            first row
     * @param auto_generate_first_column
     *            TODO
     * @throws Exception
     */
    public TableDataIterable(Table catalog_tbl, File table_file, boolean has_header, boolean auto_generate_first_column) throws Exception {
        this.catalog_tbl = catalog_tbl;
        this.table_file = table_file;
        this.auto_generate_first_column = auto_generate_first_column;
        this.reader = new CSVReader(FileUtil.getReader(this.table_file));

        // Throw away the first row if there is a header
        if (has_header) {
            this.reader.readNext();
            this.line_ctr++;
        }

        // Column Types + Foreign Keys
        // Determine whether the column references a foreign key, and thus will
        // need to be converted to an integer field later
        this.types = new VoltType[catalog_tbl.getColumns().size()];
        this.fkeys = new boolean[this.types.length];
        this.nullable = new boolean[this.types.length];
        for (Column catalog_col : catalog_tbl.getColumns()) {
            int idx = catalog_col.getIndex();
            this.types[idx] = VoltType.get((byte) catalog_col.getType());
            this.fkeys[idx] = (CatalogUtil.getForeignKeyParent(catalog_col) != null);
            this.nullable[idx] = catalog_col.getNullable();
        } // FOR
    }

    /**
     * Constructor
     * 
     * @param catalog_tbl
     * @param table_file
     * @throws Exception
     */
    public TableDataIterable(Table catalog_tbl, File table_file) throws Exception {
        this(catalog_tbl, table_file, false, false);
    }

    public Iterator<Object[]> iterator() {
        return (new TableIterator());
    }

    public class TableIterator implements Iterator<Object[]> {
        String[] next = null;

        private void getNext() {
            if (next == null) {
                try {
                    next = reader.readNext();
                } catch (Exception ex) {
                    throw new RuntimeException("Unable to retrieve tuples from '" + table_file + "'", ex);
                }
            }
        }

        @Override
        public boolean hasNext() {
            this.getNext();
            return (next != null);
        }

        @Override
        public Object[] next() {
            this.getNext();
            if (next == null)
                return (next);
            String row[] = null;
            synchronized (this) {
                row = this.next;
                this.next = null;
            } // SYNCH

            Object tuple[] = new Object[types.length];
            int row_idx = 0;
            for (int col_idx = 0; col_idx < types.length; col_idx++) {
                Column catalog_col = catalog_tbl.getColumns().get(col_idx);
                assert (catalog_col != null) : "The column at position " + col_idx + " for " + catalog_tbl + " is null";

                // Auto-generate first column
                if (col_idx == 0 && auto_generate_first_column) {
                    tuple[col_idx] = new Long(line_ctr);
                }
                // Null Values
                else if (row_idx >= row.length) {
                    tuple[col_idx] = null;
                }
                // Foreign Keys
                else if (fkeys[col_idx]) {
                    tuple[col_idx] = row[row_idx++];
                }
                // Timestamps
                else if (types[col_idx] == VoltType.TIMESTAMP) {
                    for (DateFormat f : timestamp_formats) {
                        try {
                            tuple[col_idx] = f.parse(row[row_idx]);
                        } catch (ParseException ex) {
                            // Ignore...
                        }
                        if (tuple[col_idx] != null)
                            break;
                    } // FOR
                    if (tuple[col_idx] == null) {
                        throw new RuntimeException("Line " + TableDataIterable.this.line_ctr + ": Invalid timestamp format '" + row[row_idx] + "' for " + catalog_col);
                    }
                    row_idx++;
                }
                // Store string (truncate if necessary)
                else if (types[col_idx] == VoltType.STRING) {
                    // Clip columns that are larger than our limit
                    int limit = catalog_col.getSize();
                    if (row[row_idx].length() > limit) {
                        if (!truncate_warnings.contains(catalog_col)) {
                            LOG.warn("Line " + TableDataIterable.this.line_ctr + ": Truncating data for " + catalog_col.fullName() + " because size " + row[row_idx].length() + " > " + limit);
                            truncate_warnings.add(catalog_col);
                        }
                        row[row_idx] = row[row_idx].substring(0, limit);
                    }
                    tuple[col_idx] = row[row_idx++];
                }
                // Default: Cast the string into the proper type
                else {
                    if (row[row_idx].isEmpty() && nullable[col_idx]) {
                        tuple[col_idx] = null;
                    } else {
                        try {
                            tuple[col_idx] = VoltTypeUtil.getObjectFromString(types[col_idx], row[row_idx]);
                        } catch (Exception ex) {
                            throw new RuntimeException("Line " + TableDataIterable.this.line_ctr + ": Invalid value for " + catalog_col, ex);
                        }
                    }
                    row_idx++;
                }
                // System.out.println(col_idx + ": " + tuple[col_idx]);
            } // FOR
            TableDataIterable.this.line_ctr++;
            return (tuple);
        }

        @Override
        public void remove() {
            // TODO Auto-generated method stub

        }
    }
}
