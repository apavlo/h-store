package org.voltdb.benchmark.tpcc.procedures;

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
    
    public final SQLStmt WarehouseCount = new SQLStmt("SELECT COUNT(*) FROM WAREHOUSE");
    public final SQLStmt DistrictCount  = new SQLStmt("SELECT COUNT(D_ID) FROM DISTRICT");
    public final SQLStmt CustomerCount  = new SQLStmt("SELECT COUNT(C_ID) FROM CUSTOMER");
    public final SQLStmt OrdersCount    = new SQLStmt("SELECT COUNT(O_ID) FROM ORDERS");
    public final SQLStmt NewOrderCount  = new SQLStmt("SELECT COUNT(NO_W_ID) FROM NEW_ORDER");
    public final SQLStmt OrderLineCount = new SQLStmt("SELECT COUNT(OL_O_ID) FROM ORDER_LINE");
    public final SQLStmt HistoryCount   = new SQLStmt("SELECT COUNT(H_W_ID) FROM HISTORY");
    public final SQLStmt ItemCount      = new SQLStmt("SELECT COUNT(I_ID) FROM ITEM");
    public final SQLStmt StockCount     = new SQLStmt("SELECT COUNT(S_W_ID) FROM STOCK");
    
    private final HashMap<String, SQLStmt> table_map = new HashMap<String, SQLStmt>();
    {
        this.table_map.put("WAREHOUSE", WarehouseCount);
        this.table_map.put("DISTRICT", DistrictCount);
        this.table_map.put("CUSTOMER", CustomerCount);
        this.table_map.put("ORDERS", OrdersCount);
        this.table_map.put("NEW_ORDER", NewOrderCount);
        this.table_map.put("ORDER_LINE", OrderLineCount);
        this.table_map.put("HISTORY", HistoryCount);
        this.table_map.put("ITEM", ItemCount);
        // this.table_map.put("STOCK", StockCount);
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
            boolean adv = results[0].advanceRow();
            assert(adv) : "Unable to advance results row for table " + e.getKey();
            ret.addRow(e.getKey(), results[0].getLong(0));
        } // FOR
        return (new VoltTable[]{ ret });
    }
}
