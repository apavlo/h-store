package edu.brown.fake;

import edu.brown.fake.api.Table;
import edu.brown.fake.api.Table.Row;
import edu.brown.fake.api.*;
public class foo extends StoredProcedure {
  HStoreAPI HStore = new HStoreAPI();
    
  public final String Get =
    "SELECT * FROM table1 WHERE id = ?";
  
  public final String Update =
    "UPDATE table2 SET val = val + ?" +
    " WHERE id = ?";
  
  public int run(int t1_id, double val) {
    Table orders = HStore.executeSQL(Get, t1_id);
    for (Row record : orders.getRows()) {
       long t2_id = record.get("T2_ID");
       HStore.queueSQL(Update, t2_id);
    }
    int result = HStore.executeBatch();
    return (result);
} }