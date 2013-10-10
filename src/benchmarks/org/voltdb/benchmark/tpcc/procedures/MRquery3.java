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
public class MRquery3 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
                    " SELECT ol_o_id, ol_w_id, ol_d_id, SUM(ol_amount) as revenue, o_entry_d " +
                    " FROM CUSTOMER, NEW_ORDER, ORDERS, ORDER_LINE " +
                    " WHERE   c_id = o_c_id " + 
                    " and c_w_id = o_w_id " +
                    " and c_d_id = o_d_id " +
                    " and no_w_id = o_w_id " +
                    " and no_d_id = o_d_id " +
                    " and no_o_id = o_id " +
                    " and ol_w_id = o_w_id " +
                    " and ol_d_id = o_d_id " +
                    " and ol_o_id = o_id" +
                    " and o_entry_d > '2007-01-02 00:00:00.000000' " +
                    //" GROUP BY ol_o_id " +
                    " GROUP BY ol_o_id, ol_w_id, ol_d_id, o_entry_d " + // mr_transaction can not support multi-column-keys right now
                    //" ORDER BY revenue desc, o_entry_d"); // error: "ORDER BY with complex expressions not yet supported
                    " ORDER BY o_entry_d"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
                new VoltTable.ColumnInfo("o_entry_d", VoltType.TIMESTAMP),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("revenue", VoltType.FLOAT),
                new VoltTable.ColumnInfo("o_entry_d", VoltType.TIMESTAMP),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0);
        Object new_row[] = {
                key,
                row.getLong(1),
                row.getLong(2),
                row.getDouble(3),
                row.getTimestampAsTimestamp(4)
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        double sum_ol_amount = 0;
        
        VoltTableRow row = null;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            row = r;
            sum_ol_amount += row.getDouble(3);
        } // FOR

        Object new_row[] = {
                key,
                row.getLong(1),
                row.getLong(2),
                sum_ol_amount,
                row.getTimestampAsTimestamp(4)
        };
        this.reduceEmit(new_row);
    }

}



