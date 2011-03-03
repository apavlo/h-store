package edu.brown.utils;

import java.util.Random;

import junit.framework.TestCase;

public class TestStringUtil extends TestCase {

    public void testTable() {
        Random rand = new Random();
        int num_cols = 5;
        int num_rows = 10;
        
        String header[] = new String[num_cols];
        for (int i = 0; i < num_cols; i++) {
            header[i] = String.format("COL%02d", i);
        }
        
        Object rows[][] = new String[num_rows][num_cols];
        for (int i = 0; i < num_rows; i++) {
            for (int j = 0; j < num_cols; j++) {
                if (i % 2 != 0) {
                    rows[i][j] = Integer.toString(rand.nextInt(10000));
                } else {
                    rows[i][j] = Float.toString(rand.nextFloat());
                }
            }
        }
        
        String table = StringUtil.table(header, rows);
        System.err.println(table);
    }
    
}
