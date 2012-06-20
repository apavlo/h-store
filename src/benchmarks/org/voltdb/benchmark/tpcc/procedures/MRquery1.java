package org.voltdb.benchmark.tpcc.procedures;

import java.util.Iterator;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltMapReduceProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltTableRow;
import org.voltdb.VoltType;

import edu.brown.utils.CollectionUtil;

@ProcInfo(
        mapInputQuery = "mapInputQuery"
)

public class MRquery1 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
            "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), COUNT(*) " +
            "FROM order_line " +
            "WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' " +
            "GROUP BY ol_number " +
            "ORDER BY ol_number"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_quantity", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("count_order", VoltType.BIGINT),
        };
    }
    /*
     */
    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("sum_qty", VoltType.BIGINT),
                new VoltTable.ColumnInfo("sum_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("avg_qty", VoltType.BIGINT),
                new VoltTable.ColumnInfo("avg_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("count_order", VoltType.BIGINT),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0); // A_NAME
        long quantity = row.getLong(1);
        double amount = row.getDouble(2);
        long ct = row.getLong(3);
        Object new_row[] = {
                key,
                quantity, // FIXME row.getLong(1)
                amount,
                ct
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        long count = 0;
        long sum_qty = 0;
        double sum_amount = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_qty += rows.next().getLong(1);
            sum_amount += rows.next().getDouble(2);
            count+= rows.next().getLong(3);
        } // FOR

        long avg_qty = sum_qty / count;
        double avg_amount = sum_amount / count;

        Object new_row[] = {
                key,
                sum_qty,
                sum_amount,
                avg_qty,
                avg_amount,
                count
        };
        this.reduceEmit(new_row);
    }

}


