package edu.brown.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import junit.framework.TestCase;

public class TestTableUtil extends TestCase {


    private static final int num_cols = 5;
    private static final int num_rows = 10;
    private static final Random rand = new Random();
    
    private final String header[] = new String[num_cols]; 
    private final Object rows[][] = new String[num_rows][num_cols];
    
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        
        
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
        formats.add(new TableUtil.Format("X", null, null, true, true, true, true, true, false, false, true, null));
        formats.add(new TableUtil.Format("X", null, null, true, true, true, true, true, false, false, false, null));
        
        for (TableUtil.Format f : formats) {
            String table = TableUtil.table(f, header, rows);
            System.out.println(table + "\n");
        } // FOR
        System.out.println(StringUtil.DOUBLE_LINE);
    }
    
    /**
     * testReplaceNullCell
     */
    public void testReplaceNullCell() {
        int null_ctr = 0;
        for (int i = 0; i < num_rows; i++) {
            int mod = (i % 2);
            for (int j = 0; j < num_cols; j++) {
                if (j % 2 == mod) {
                    rows[i][j] = null;
                    null_ctr++;
                }
            } // FOR
        } // FOR

        Object replace_markers[] = new Object[] {
            "XXXXXX",
            31313131313l,
        };
        
        for (Object replace : replace_markers) {
            TableUtil.Format f = new TableUtil.Format(" | ", null, null, true, true, true, false, false, false, false, false, replace);
            String table = TableUtil.table(f, header, rows);
            assertNotNull(table);
            assertFalse(table.isEmpty());
            System.out.println(table + "\n");
            assertFalse(table.contains("null"));
            
            String split[] = table.split(replace.toString());
            assertEquals(null_ctr + 1, split.length);
        }

        System.out.println(StringUtil.DOUBLE_LINE);
    }
    
    /**
     * testPruneNullRows
     */
    public void testPruneNullRows() {
        Object rows[][] = new String[num_rows][];
        int not_null_cnt = 0;
        for (int i = 0; i < num_rows; i++) {
            if (i % 2 != 0) {
                rows[i] = new String[num_cols];
                for (int j = 0; j < num_cols; j++) {
                    rows[i][j] = Integer.toString(rand.nextInt(10000));
                } // FOR
                not_null_cnt++;
            }
        } // FOR
         
        for (boolean prune_null_rows : new boolean[]{ true, false }) {
            TableUtil.Format f = new TableUtil.Format(" | ", null, null, true, true, true, false, false, false, false, prune_null_rows, null);
            String table = TableUtil.table(f, header, rows);
            System.out.println(table + "\n");
            String lines[] = StringUtil.splitLines(table);
            assertEquals("prune_null_rows=" + prune_null_rows, 1 + (prune_null_rows ? not_null_cnt : num_rows), lines.length);
        } // FOR
        System.out.println(StringUtil.DOUBLE_LINE);
    }
    
    /**
     * testTableMap
     */
    public void testTableMap() {
        String col_delimiters[] = new String[num_cols];
        col_delimiters[2] = " | ";
        String row_delimiters[] = new String[num_rows];
        row_delimiters[num_rows-2] = "\u2015";
        
        TableUtil.Format f = new TableUtil.Format("   ", col_delimiters, row_delimiters, true, false, true, false, false, false, false, false, null);
        assertNotNull(f);
        Map<String, String> m = TableUtil.tableMap(f, header, rows);
        
        System.out.println(StringUtil.formatMaps(m));
        assertEquals(num_rows+2, m.size());
        System.out.println(StringUtil.DOUBLE_LINE);
    }
    
}
