package edu.brown.fake;

import edu.brown.fake.api.Result;
import edu.brown.fake.api.*;

class neworder extends StoredProcedure {
  HStore HStore = new HStore();
    
  final HStore.SQLStatement GetDistrict = new HStore.SQLStatement(
    "SELECT D_NEXT_O_ID FROM DISTRICT "
        + "WHERE D_ID = ? AND D_W_ID = ?");
  
  final HStore.SQLStatement UpdateDistrict = new HStore.SQLStatement(
    "UPDATE DISTRICT SET D_NEXT_O_ID = ? " 
        + "WHERE D_ID = ? AND D_W_ID = ?");
  
  final HStore.SQLStatement InsertOrder = new HStore.SQLStatement(
    "INSERT INTO ORDERS (O_ID, O_D_ID, O_W_ID) "
        + "VALUES (?, ?, ?)");
  
  final HStore.SQLStatement InsertOrderLine = new HStore.SQLStatement(
    "INSERT INTO ORDER_LINE (O_ID, O_D_ID, O_W_ID) " 
        + "VALUES (?, ?, ?)");
  
  public int run(int w_id, int d_id, long items[]) {
    Result r1 = HStore.executeSQL(GetDistrict, d_id, w_id);
    long o_id = r1.get("D_NEXT_O_ID") + 1;
    
    HStore.queueSQL(UpdateDistrict, o_id, d_id, w_id);
    HStore.queueSQL(InsertOrder, o_id, d_id, w_id);
    for (long i_id : items) {
        HStore.queueSQL(InsertOrderLine, i_id, o_id, d_id, w_id);
    }
    Result r2 = HStore.executeBatch();
    return (r2.rows_updated);
} }