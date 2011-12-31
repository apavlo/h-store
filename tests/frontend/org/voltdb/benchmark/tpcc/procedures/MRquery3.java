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
              //   "SELECT A_NAME FROM TABLEA WHERE A_AGE >= ?"
            "select   ol_o_id, ol_w_id, ol_d_id, " +
            "    sum(ol_amount)" +
            "from    customer, neworder, orders, orderline " +
            "where   c_state like 'A%' " +
            "    and c_id = o_c_id " +
            "    and c_w_id = o_w_id " +
            "    and c_d_id = o_d_id " +
            "    and no_w_id = o_w_id " +
            "    and no_d_id = o_d_id " +
            "    and no_o_id = o_id " +
            "    and ol_w_id = o_w_id " +
            "    and ol_d_id = o_d_id " +
            "    and ol_o_id = o_id " +
            "group by ol_o_id, ol_w_id, ol_d_id"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_o_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_w_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_d_id", VoltType.BIGINT),
                new VoltTable.ColumnInfo("revenue", VoltType.FLOAT),
        };
    }

    @Override
    public void map(VoltTableRow row) {
        long key = row.getLong(0);
        Object new_row[] = {
                key,
                row.getLong(1),
                row.getLong(2),
                row.getDouble(3)
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        double sum_ol_amount = 0;
        
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_ol_amount += rows.next().getDouble(3);
        } // FOR

        Object new_row[] = {
                key,
                rows.next().getLong(1),
                rows.next().getLong(2),
                sum_ol_amount
        };
        this.reduceEmit(new_row);
    }

}



