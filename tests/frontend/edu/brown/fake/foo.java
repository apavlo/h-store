package edu.brown.fake;

import edu.brown.fake.api.HStore;
import edu.brown.fake.api.Result;
import edu.brown.fake.api.Result.Row;
import edu.brown.fake.api.*;
class foo extends StoredProcedure {
  HStore HStore = new HStore();
    
  final HStore.SQLStatement Get = new HStore.SQLStatement(
    "SELECT * FROM t1 WHERE id = ?");
  
  final HStore.SQLStatement Update = new HStore.SQLStatement(
    "UPDATE t2 SET val = ? WHERE id = ?");
  
  public int run(int t1_id, double val) {
    Result r1 = HStore.executeSQL(Get, t1_id);
    for (Row record : r1.getRows()) {
       long t2_id = record.get("T2_ID");
       HStore.queueSQL(Update, t2_id, val);
    }
    Result r2 = HStore.executeBatch();
    return (r2.rows_updated);
} }