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
public class MRquery12 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
//            "SELECT ol_number, SUM(ol_quantity), SUM(ol_amount), COUNT(*) " +
//                    "FROM order_line " +
//                    "WHERE ol_delivery_d > '2007-01-02 00:00:00.000000' " +
//                    "GROUP BY ol_number " +
//                    "ORDER BY ol_number"
            "SELECT o_ol_cnt, o_carrier_id " +
            "FROM ORDERS, ORDER_LINE " +
            "WHERE ol_w_id = o_w_id " +
            " and ol_d_id = o_d_id " + 
            " and ol_o_id = o_id " +
            " and o_entry_d <= ol_delivery_d " + 
            " and ol_delivery_d < '2020-01-01 00:00:00.000000' " + 
            //"GROUP BY o_ol_cnt " +
            "ORDER BY o_ol_cnt"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("high_line_count", VoltType.BIGINT),
                new VoltTable.ColumnInfo("low_line_count", VoltType.BIGINT),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("revenue", VoltType.BIGINT),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0); 
        long line = row.getLong(1);
        long high_line_ct = 0; 
        long low_line_ct = 0;
        if (line == 1 || line == 2) high_line_ct = 1;
        if (line != 1 && line != 2) low_line_ct = 1;
        Object new_row[] = {
                key,
                high_line_ct,
                low_line_ct
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        long sum_high = 0;
        long sum_low = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_high += rows.next().getLong(1);
            sum_low += rows.next().getLong(2);
        } // FOR

        Object new_row[] = {
                key,
                sum_high,
                sum_low
        };
        this.reduceEmit(new_row);
    }

}



