package org.voltdb.utils;

import java.util.Iterator;
import java.util.Random;

import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import edu.brown.statistics.Histogram;
import edu.brown.statistics.ObjectHistogram;
import edu.brown.utils.CollectionUtil;

import junit.framework.TestCase;

public class TestReduceInputIterator extends TestCase {
    
    static final VoltTable.ColumnInfo[] SCHEMA = new VoltTable.ColumnInfo[] {
        //new VoltTable.ColumnInfo("ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        //new VoltTable.ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    static final int NUM_ROWS = 10;
    static final Random rand = new Random();

    private VoltTable table = new VoltTable(SCHEMA);
    private VoltTable reduceOutput = new VoltTable(SCHEMA);
    private Histogram<String> keyHistogram = new ObjectHistogram<String>(); 
    private Histogram<Long> countHistogram = new ObjectHistogram<Long>(); 
       
    @Override
    protected void setUp() throws Exception {
        String key = "";
        for (int i = 0; i < NUM_ROWS; i++) {
            String name="Jason";
            if(i <3) name="Jason00";
            else if(i <4) name="David01";
            else name = "Tomas77";
            
            long ct = 3;
            if(key!=name) key = "";
            if(key == "") {
                keyHistogram.put(name);
                key = name;
                countHistogram.put(ct);
            }
            
            Object row[] = {name,ct};
            this.table.addRow(row);
            
        } // FOR
        assertEquals(NUM_ROWS, this.table.getRowCount());
    }
    
    /**
     * testHasKey
     */
    public void testHasKey() throws Exception {
        ReduceInputIterator<Long> iterator = new ReduceInputIterator<Long>(this.table);
        assertNotNull(iterator);
        assertTrue(iterator.hasKey());
    }
    
    /*
     * test hasNext
     */
    public void testHasNext() throws Exception {
        ReduceInputIterator<String> rows = new ReduceInputIterator<String>(this.table);
        assertNotNull(rows);
        
        Histogram<String> actualkey = new ObjectHistogram<String>();
        Histogram<Long> actualcount = new ObjectHistogram<Long>();
        //System.out.println("Input table:\n" + this.table);
        while (rows.hasNext()) {
            String key = rows.getKey();
            Long ct = rows.next().getLong(1);
            this.reduce(key, rows);
            actualkey.put(key);
            actualcount.put(ct);
        }
        //System.out.println("Output table:\n" + this.reduceOutput);
        
        for (String key : keyHistogram.values()) {
            assertEquals(keyHistogram.get(key), actualkey.get(key));
        }
        for (Long val : countHistogram.values()) {
            assertEquals(countHistogram.get(val), actualcount.get(val));
        }
       
    }
    
    public void reduce(String key, Iterator<VoltTableRow> rows) {
        long count = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            //Long ct = (Long)r.getLong(1);
            long ct = (long)r.getLong(1);
            count+=ct;

            //System.out.println("....key is: " + key + " Value: "+count);
        } // FOR
      
//        while (rows.hasNext()) {
//            System.out.println("[row]: "+ key);
//            count++;
//        }
        
        Object new_row[] = {
            key,
            count
        };
        
        this.reduceOutput.addRow(new_row);
    }

}
