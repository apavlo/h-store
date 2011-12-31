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
public class MRquery19 extends VoltMapReduceProcedure<Long> {

    public SQLStmt mapInputQuery = new SQLStmt(
            "select    ol_number, sum(ol_amount) " +
            "from   order_line " +
            "where  ( " +
//            "     ol_i_id = i_id " +
//            "     and i_data like '%a' " +
            "     ol_o_id >= 20 " +
            "     and ol_o_id <= 100 " +
//            "     and i_price between 1 and 20000 " +
//            "     and ol_w_id in (1,2,3) " +
            "   ) or ( " +
//            "     ol_i_id = i_id " +
//            "     and i_data like '%b' " +
            "     ol_o_id >= 105 " +
            "     and ol_o_id <= 200 " +
//            "     and i_price between 80000 and 100000 " +
//            "     and ol_w_id in (1,2,4) " +
            "   ) or ( " +
//            "     ol_i_id = i_id " +
//            "     and i_data like '%c' " +
            "     ol_o_id >= 210 " +
            "     and ol_o_id <= 290 " +
//            "     and i_price between 200000 and 250000 " +
//            "     and ol_w_id in (1,5,3) " +
            "   ) " +
            "GROUP BY ol_number order by ol_number"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
                new VoltTable.ColumnInfo("ol_number", VoltType.BIGINT),
                new VoltTable.ColumnInfo("ol_amount", VoltType.FLOAT),
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
        Object new_row[] = {
                key,
                row.getDouble(1)
        };
        this.mapEmit(key, new_row);
    }

    @Override
    public void reduce(Long key, Iterator<VoltTableRow> rows) {
        double sum_ol_amount = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            sum_ol_amount += rows.next().getDouble(1);
        } // FOR

        Object new_row[] = {
                key,
                sum_ol_amount
        };
        this.reduceEmit(new_row);
    }

}



