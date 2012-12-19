package edu.brown.benchmark.wikipedia.procedures;

import java.util.HashMap;
import java.util.Map.Entry;

import org.voltdb.ProcInfo;
import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.VoltType;

@ProcInfo(
    singlePartition = false
)
public class GetTableCounts extends VoltProcedure {
    
    public final SQLStmt ipblocks = new SQLStmt("SELECT COUNT(*) FROM IPBLOCKS");
    public final SQLStmt useracct = new SQLStmt("SELECT COUNT(*) FROM USERACCT");
    public final SQLStmt logging = new SQLStmt("SELECT COUNT(*) FROM LOGGING");
    public final SQLStmt page = new SQLStmt("SELECT COUNT(*) FROM PAGE");
    public final SQLStmt page_restrictions = new SQLStmt("SELECT COUNT(*) FROM PAGE_RESTRICTIONS");
    public final SQLStmt recentchanges = new SQLStmt("SELECT COUNT(*) FROM RECENTCHANGES");
    public final SQLStmt text = new SQLStmt("SELECT COUNT(*) FROM TEXT");
    public final SQLStmt revision = new SQLStmt("SELECT COUNT(*) FROM REVISION");
    public final SQLStmt user_groups = new SQLStmt("SELECT COUNT(*) FROM USER_GROUPS");
    public final SQLStmt watchlist = new SQLStmt("SELECT COUNT(*) FROM WATCHLIST");
    
    private final HashMap<String, SQLStmt> table_map = new HashMap<String, SQLStmt>();
    {
        this.table_map.put("IPBLOCKS", ipblocks);
        this.table_map.put("USERACCT", useracct);
        this.table_map.put("LOGGING", logging);
        this.table_map.put("PAGE", page);
        this.table_map.put("PAGE_RESTRICTIONS", page_restrictions);
        this.table_map.put("RECENTCHANGES", recentchanges);
        this.table_map.put("TEXT", text);
        this.table_map.put("REVISION", revision);
        this.table_map.put("USER_GROUPS", user_groups);
        this.table_map.put("WATCHLIST", watchlist);
    }
    
    private final VoltTable.ColumnInfo columns[] = {
        new VoltTable.ColumnInfo("name", VoltType.STRING),
        new VoltTable.ColumnInfo("size", VoltType.BIGINT),
    };
    
    public VoltTable[] run() {
        VoltTable ret = new VoltTable(this.columns);
        for (Entry<String, SQLStmt> e : this.table_map.entrySet()) {
            // System.err.println("Retrieving number of tuples for " + e.getKey());
            voltQueueSQL(e.getValue());
            VoltTable results[] = voltExecuteSQL();
            assert(results.length == 1) : "Got " + results.length + " results for table " + e.getKey();
            assert(results[0].getRowCount() > 0);
            assert(results[0].advanceRow()) : "Unable to advance results row for table " + e.getKey();
            ret.addRow(e.getKey(), results[0].getLong(0));
        } // FOR
        return (new VoltTable[]{ ret });
    }
}