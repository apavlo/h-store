package org.voltdb.utils;

import java.util.Comparator;

import org.voltdb.VoltTable;
import org.voltdb.VoltType;
import org.voltdb.types.SortDirectionType;
import org.voltdb.types.TimestampType;

/**
 * @author pavlo
 */
public class VoltTableComparator implements Comparator<Object[]> {

    private final VoltTable table;
    private final Pair<Integer, SortDirectionType> cols[];
    private final VoltType types[];

    public VoltTableComparator(VoltTable table, Pair<Integer, SortDirectionType> cols[]) {
        this.table = table;
        this.cols = cols;

        this.types = new VoltType[this.cols.length];
        for (int i = 0; i < this.types.length; i++) {
            this.types[i] = this.table.getColumnType(cols[i].getFirst());
        } // FOR
    }

    @Override
    public int compare(Object[] o1, Object[] o2) {
        assert (o1 != null);
        assert (o2 != null);
        assert (o1.length == o2.length);
        int cmp = 0;
        int sort_idx = 0;
        for (Pair<Integer, SortDirectionType> p : this.cols) {
            int col_idx = p.getFirst();
            assert (col_idx < o1.length);
            SortDirectionType dir = p.getSecond();
            assert (dir != SortDirectionType.INVALID);

            switch (this.types[sort_idx]) {
                case TINYINT:
                    cmp = ((Byte) o1[col_idx]) - ((Byte) o2[col_idx]);
                    break;
                case SMALLINT:
                    cmp = ((Short) o1[col_idx]) - ((Short) o2[col_idx]);
                    break;
                case INTEGER:
                    cmp = ((Integer) o1[col_idx]) - ((Integer) o2[col_idx]);
                    break;
                case BIGINT:
                    cmp = (int) (((Long) o1[col_idx]) - ((Long) o2[col_idx]));
                    break;
                case FLOAT:
                    cmp = (int) (((Float) o1[col_idx]) - ((Float) o2[col_idx]));
                    break;
                case DECIMAL:
                    cmp = (int) (((Double) o1[col_idx]) - ((Double) o2[col_idx]));
                    break;
                case STRING:
                    cmp = ((String) o1[col_idx]).compareTo((String) o2[col_idx]);
                    break;
                case TIMESTAMP:
                    cmp = ((TimestampType) o1[col_idx]).compareTo((TimestampType) o2[col_idx]);
                    break;
                case BOOLEAN:
                    cmp = ((Boolean) o1[col_idx]).compareTo((Boolean) o2[col_idx]);
                    break;
                default:
                    assert (false) : "Unsupported sorting column type " + this.types[sort_idx];
            } // SWITCH
            if (cmp != 0)
                break;
            sort_idx++;
        } // FOR

        // TODO: Handle duplicates!
        return (cmp);
    }
}
