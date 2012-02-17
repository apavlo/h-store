package edu.brown.benchmark.mapreduce.procedures;

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
public class MockMapReduce extends VoltMapReduceProcedure<String> {

    public SQLStmt mapInputQuery = new SQLStmt(
        "SELECT A_NAME, COUNT(*) FROM TABLEA WHERE A_AGE >= ? GROUP BY A_NAME"
    );

    @Override
    public VoltTable.ColumnInfo[] getMapOutputSchema() {
        return new VoltTable.ColumnInfo[]{
            new VoltTable.ColumnInfo("NAME", VoltType.STRING),
            new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        };
    }
    
    @Override
    public VoltTable.ColumnInfo[] getReduceOutputSchema() {
        return new VoltTable.ColumnInfo[]{
            new VoltTable.ColumnInfo("NAME", VoltType.STRING),
            new VoltTable.ColumnInfo("COUNTER", VoltType.BIGINT),
        };
    }
    
    
    @Override
    public void map(VoltTableRow row) {
        String key = row.getString(0); // A_NAME
        Object new_row[] = {
            key,
            row.getLong(1)
        };
        this.mapEmit(key, new_row); // mapOutputTable
    }
    
    @Override
    public void reduce(String key, Iterator<VoltTableRow> rows) {
        long count = 0;
        for (VoltTableRow r : CollectionUtil.iterable(rows)) {
            assert(r != null);
            long ct = (long)r.getLong(1);
            count+=ct;
        } // FOR

        Object new_row[] = {
            key,
            count
        };
        this.reduceEmit(new_row);// reduceOutput table
    }


}
