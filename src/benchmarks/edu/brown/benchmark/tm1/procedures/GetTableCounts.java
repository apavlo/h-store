package edu.brown.benchmark.tm1.procedures;

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

import edu.brown.benchmark.tm1.TM1Constants;

@ProcInfo(singlePartition = false)
public class GetTableCounts extends VoltProcedure {

    private final String base_stmt = "SELECT COUNT(S_ID) FROM ";
    public final SQLStmt SubscriberCount = new SQLStmt(base_stmt + TM1Constants.TABLENAME_SUBSCRIBER);
    public final SQLStmt AccessInfoCount = new SQLStmt(base_stmt + TM1Constants.TABLENAME_ACCESS_INFO);
    public final SQLStmt SpecialFacilityCount = new SQLStmt(base_stmt + TM1Constants.TABLENAME_SPECIAL_FACILITY);
    public final SQLStmt CallForwardingCount = new SQLStmt(base_stmt + TM1Constants.TABLENAME_CALL_FORWARDING);

    private final HashMap<String, SQLStmt> table_map = new HashMap<String, SQLStmt>();
    {
        this.table_map.put(TM1Constants.TABLENAME_SUBSCRIBER, SubscriberCount);
        this.table_map.put(TM1Constants.TABLENAME_ACCESS_INFO, AccessInfoCount);
        this.table_map.put(TM1Constants.TABLENAME_SPECIAL_FACILITY, SpecialFacilityCount);
        this.table_map.put(TM1Constants.TABLENAME_CALL_FORWARDING, CallForwardingCount);
    }

    private final VoltTable.ColumnInfo columns[] = { new VoltTable.ColumnInfo("name", VoltType.STRING), new VoltTable.ColumnInfo("size", VoltType.BIGINT),
            new VoltTable.ColumnInfo("min", VoltType.BIGINT), new VoltTable.ColumnInfo("max", VoltType.BIGINT), };

    public VoltTable[] run() {
        VoltTable ret = new VoltTable(this.columns);
        for (Entry<String, SQLStmt> e : this.table_map.entrySet()) {
            // if (!e.getKey().equals(TM1Constants.TABLENAME_SPECIAL_FACILITY))
            // continue;
            System.err.println("Retrieving number of tuples for " + e.getKey());
            voltQueueSQL(e.getValue());
            VoltTable results[] = voltExecuteSQL();
            assert (results.length == 1) : "Got " + results.length + " results for table " + e.getKey();
            assert (results[0].getRowCount() > 0);
            boolean adv = results[0].advanceRow();
            assert (adv) : "Unable to advance results row for table " + e.getKey();
            ret.addRow(e.getKey(), results[0].getLong(0), results[0].getLong(0), results[0].getLong(0));
        } // FOR
        return (new VoltTable[] { ret });
    }
}
