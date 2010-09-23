/**
 * 
 */
package edu.brown.utils;

import java.io.*;
import java.text.*;
import java.util.*;
import java.util.regex.*;

import org.apache.log4j.Logger;
import org.voltdb.VoltType;
import org.voltdb.catalog.*;
import org.voltdb.utils.VoltTypeUtil;
import org.voltdb.utils.CatalogUtil;

import edu.brown.utils.FileUtil;

/**
 * @author pavlo
 *
 */
public class TableDataIterable implements Iterable<Object[]> {
    private static final Logger LOG = Logger.getLogger(TableDataIterable.class.getName());
    
    private final Table catalog_tbl;
    private final String table_file;
    private final BufferedReader reader;
    private final VoltType types[];
    private final boolean fkeys[];
    private final boolean nullable[];
    
    private final DateFormat timestamp_formats[] = new DateFormat[] {
        new SimpleDateFormat("yyyy-MM-dd"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"),
        new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"),
    };
    private final Pattern p = Pattern.compile("\\|");
    private Set<Column> truncate_warnings = new HashSet<Column>();
    private int line_ctr = 0;
    
    /**
     * Constructor
     * @param catalog_tbl
     * @param table_file
     * @param has_header whether we expect the data file to include a header in the first row
     * @throws Exception
     */
    public TableDataIterable(Table catalog_tbl, String table_file, boolean has_header) throws Exception {
        this.catalog_tbl = catalog_tbl;
        this.table_file = table_file;
        this.reader = FileUtil.getReader(this.table_file);
        
        // Throw away the first row if there is a header
        if (has_header) {
            this.reader.readLine();
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
            this.types[idx] = VoltType.get((byte)catalog_col.getType());
            this.fkeys[idx] = (CatalogUtil.getForeignKeyParent(catalog_col) != null);
            this.nullable[idx] = catalog_col.getNullable();
        } // FOR
    }
    
    /**
     * Constructor
     * @param catalog_tbl
     * @param table_file
     * @throws Exception
     */
    public TableDataIterable(Table catalog_tbl, String table_file) throws Exception {
        this(catalog_tbl, table_file, false);
    }
    
    public Iterator<Object[]> iterator() {
        return (new TableIterator());
    }
    
    public class TableIterator implements Iterator<Object[]> {
        @Override
        public boolean hasNext() {
            boolean ret = false;
            try {
                ret = reader.ready();
            } catch (Exception ex) {
                LOG.error("Unable to retrieve tuples from '" + table_file + "'", ex);
                System.exit(1);
            }
            return (ret);
        }
        
        @Override
        public Object[] next() {
            String line = null;
            try {
                line = reader.readLine();
            } catch (Exception ex) {
                LOG.error("Unable to retrieve tuples from '" + table_file + "'", ex);
                System.exit(1);
            }
            //
            // Split the record by the delimiter and then cast all the the columns
            // to their proper type
            //
            String data[] = p.split(line);
            // if (table_file.contains("airline")) System.out.println(line + " [" + data.length + "]");
            Object tuple[] = new Object[types.length];
            for (int i = 0; i < types.length; i++) {
                Column catalog_col = catalog_tbl.getColumns().get(i);
                assert(catalog_col != null) : "The column at position " + i + " for " + catalog_tbl + " is null";
                
                //
                // Null Values
                //
                if (i >= data.length) {
                    tuple[i] = null;
                //
                // Foreign Keys
                //
                } else if (fkeys[i]) {
                    tuple[i] = data[i];
                //
                // Timestamps
                //
                } else if (types[i] == VoltType.TIMESTAMP) {
                    for (DateFormat f : timestamp_formats) {
                        try {
                            tuple[i] = f.parse(data[i]);
                        } catch (ParseException ex) {
                            // Ignore...
                        }
                        if (tuple[i] != null) break;
                    } // FOR
                    if (tuple[i] == null) {
                        LOG.error("Line " + TableDataIterable.this.line_ctr + ": Invalid timestamp format '" + data[i] + "' for " + catalog_col);
                        System.exit(1);
                    }
                //
                // Store string (truncate if necessary)
                //
                } else if (types[i] == VoltType.STRING) {
                    //
                    // Clip columns that are larger than our limit
                    //
                    int limit = catalog_col.getSize();
                    if (data[i].length() > limit) {
                        if (!truncate_warnings.contains(catalog_col)) {
                            LOG.warn("Line " + TableDataIterable.this.line_ctr + ": Truncating data for " + catalog_tbl.getName() + "." + catalog_col.getName() +
                                     " because size " + data[i].length() + " > " + limit);
                            truncate_warnings.add(catalog_col);
                        }
                        data[i] = data[i].substring(0, limit);
                    }
                    tuple[i] = data[i];
                    
                //
                // Default: Cast the string into the proper type
                //
                } else {
                    if (data[i].isEmpty() && nullable[i]) {
                        tuple[i] = null;
                    } else {
                        try {
                            tuple[i] = VoltTypeUtil.getObjectFromString(types[i], data[i]);
                        } catch (Exception ex) {
                            LOG.error("Line " + TableDataIterable.this.line_ctr + ": Invalid value for " + catalog_col, ex);
                            System.exit(1);
                        }
                    }
                }
                // System.out.println(i + ": " + tuple[i]);
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
