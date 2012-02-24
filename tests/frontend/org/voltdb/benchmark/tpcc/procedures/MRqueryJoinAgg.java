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

public class MRqueryJoinAgg extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
            "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), SUM(i_price), COUNT(*) " +
            "FROM order_line, item " +
            "WHERE order_line.ol_i_id = item.i_id " +
            "GROUP BY ol_number " +
            "ORDER BY ol_number"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_quantity", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("i_price", VoltType.FLOAT),
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
                new VoltTable.ColumnInfo("sum_price", VoltType.FLOAT),
                new VoltTable.ColumnInfo("avg_qty", VoltType.BIGINT),
                new VoltTable.ColumnInfo("avg_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("avg_price", VoltType.FLOAT),
                new VoltTable.ColumnInfo("count_order", VoltType.BIGINT),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0); // A_NAME
        long quantity = row.getLong(1);
        double amount = row.getDouble(2);
        double price = row.getDouble(3);
        long ct = row.getLong(4);
        Object new_row[] = {
                key,
                quantity, // FIXME row.getLong(1)
                amount,
                price,
                ct
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        long ct = 0;
        long sum_qty = 0;
        double sum_amount = 0;
        double sum_price = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_qty += rows.next().getLong(1);
            sum_amount += rows.next().getDouble(2);
            sum_price += rows.next().getDouble(3);
            ct+= rows.next().getLong(4);
        } // FOR

        long avg_qty = sum_qty / ct;
        double avg_amount = sum_amount / ct;
        double avg_price = sum_price / ct;

        Object new_row[] = {
                key,
                sum_qty,
                sum_amount,
                sum_price,
                avg_qty,
                avg_amount,
                avg_price,
                ct
        };
        this.reduceEmit(new_row);
    }

}


