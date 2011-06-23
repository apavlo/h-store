package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

public class TestTableUtil extends TestCase {


    int num_cols = 5;
    int num_rows = 10;

    private final String header[] = new String[num_cols]; 
    private final Object rows[][] = new String[num_rows][num_cols];
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        Random rand = new Random();
        for (int i = 0; i < num_cols; i++) {
            header[i] = String.format("COL%02d", i);
        }
        for (int i = 0; i < num_rows; i++) {
            for (int j = 0; j < num_cols; j++) {
                if (i % 2 != 0) {
                    rows[i][j] = Integer.toString(rand.nextInt(10000));
                } else {
                    rows[i][j] = String.format("%.8f", rand.nextFloat());
                }
            }
        }
    }
    
    /**
     * testTableFormat
     */
    public void testTableFormat() {
        List<TableUtil.Format> formats = new ArrayList<TableUtil.Format>();
        formats.add(TableUtil.defaultTableFormat());
        formats.add(TableUtil.defaultCSVFormat());
        formats.add(new TableUtil.Format("X", null, null, true, true, true, true, true, false));
        
        for (TableUtil.Format f : formats) {
            String table = TableUtil.table(f, header, rows);
            System.err.println(table + "\n");
        } // FOR
    }
    
    /**
     * testTableMap
     */
    public void testTableMap() {
        String col_delimiters[] = new String[num_cols];
        col_delimiters[2] = " | ";
        String row_delimiters[] = new String[num_rows];
        row_delimiters[num_rows-2] = "\u2015";
        
        TableUtil.Format f = new TableUtil.Format("   ", col_delimiters, row_delimiters, true, false, true, false, false, false);
        assertNotNull(f);
        Map<String, String> m = TableUtil.tableMap(f, header, rows);
        
        System.err.println(StringUtil.formatMaps(m));
        assertEquals(num_rows+2, m.size());
    }
    
}
