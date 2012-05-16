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
public class MRquery6 extends VoltMapReduceProcedure<Double> {
    // It's silly for me to do this query for MR transaction, simple sum with only one key
    public SQLStmt mapInputQuery = new SQLStmt(
            "SELECT SUM(ol_amount) as revenue " +
            "FROM ORDER_LINE " +
            "WHERE ol_delivery_d >= '1999-01-01 00:00:00.000000' " +
                "and ol_delivery_d < '2020-01-01 00:00:00.000000' " +
                "and ol_quantity between 1 and 100000"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("key", VoltType.FLOAT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
        };
    }

    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("revenue", VoltType.FLOAT)
        };
    }

    @Override
    public void map(VoltTableRow row) {
        double value = row.getDouble(0);
        Object new_row[] = {
                0.1,
                value
        };
        this.mapEmit(0.1, new_row);
    }

    @Override
    public void reduce(Double key, Iterator<VoltTableRow> rows) {
        double sum_ol_amount = 0;
        
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_ol_amount += rows.next().getDouble(1);
        } // FOR

        Object new_row[] = {
                sum_ol_amount
        };
        this.reduceEmit(new_row);
    }

}



