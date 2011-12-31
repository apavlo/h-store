package org.voltdb.utils;

import java.util.Random;

import org.junit.Test;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.SortDirectionType;

import junit.framework.TestCase;

/**
 * @author pavlo
 */
public class TestVoltTableUtil extends TestCase {

    static final VoltTable.ColumnInfo[] SCHEMA = new VoltTable.ColumnInfo[] {
        new VoltTable.ColumnInfo("ID", VoltType.BIGINT),
        new VoltTable.ColumnInfo("NAME", VoltType.STRING),
        new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        new VoltTable.ColumnInfo("CREATED", VoltType.TIMESTAMP)
    };

    static final int NUM_ROWS = 10;
    static final Random rand = new Random();

    private VoltTable table = new VoltTable(SCHEMA);

    @Override
    protected void setUp() throws Exception {
        for (int i = 0; i < NUM_ROWS; i++) {
            Object row[] = new Object[SCHEMA.length];
            for (int j = 0; j < row.length; j++) {
                row[j] = VoltTypeUtil.getRandomValue(SCHEMA[j].getType());
            } // FOR
            this.table.addRow(row);
        } // FOR
        assertEquals(NUM_ROWS, this.table.getRowCount());
    }

    /**
     * testSimpleSort
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testSimpleSort() throws Exception {
        for (int i = 0; i < SCHEMA.length; i++) {
            Pair<Integer, SortDirectionType> sortCol = Pair.of(i, SortDirectionType.ASC);
            VoltTable sorted = VoltTableUtil.sort(this.table, sortCol);
            assertNotNull(sorted);
            assertEquals(this.table.getRowCount(), sorted.getRowCount());

            System.err.println(sorted);

            Comparable last = null;
            while (sorted.advanceRow()) {
                Comparable cur = (Comparable<?>) sorted.get(sortCol.getFirst());
                if (last != null) {
                    assert (cur.compareTo(last) > 0) : String.format("%s > %s", cur, last);
                }
                last = cur;
            } // WHILE
        } // FOR
    }
    
    /**
     * testDuplicates
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Test
    public void testDuplicates() throws Exception {
        int ctr = 0;
        while (this.table.advanceRow() && ctr++ < NUM_ROWS) {
            Object row[] = this.table.getRowArray();
            this.table.addRow(row);
        } // WHILE
        assertEquals(NUM_ROWS*2, this.table.getRowCount());
        
        for (int i = 0; i < SCHEMA.length; i++) {
            Pair<Integer, SortDirectionType> sortCol = Pair.of(i, SortDirectionType.ASC);
            VoltTable sorted = VoltTableUtil.sort(this.table, sortCol);
            assertNotNull(sorted);
            assertEquals(this.table.getRowCount(), sorted.getRowCount());

            System.err.println(sorted);

            Comparable last = null;
            while (sorted.advanceRow()) {
                Comparable cur = (Comparable<?>) sorted.get(sortCol.getFirst());
                if (last != null) {
                    assert (cur.compareTo(last) >= 0) : String.format("%s > %s", cur, last);
                }
                last = cur;
            } // WHILE
        } // FOR
    }
}