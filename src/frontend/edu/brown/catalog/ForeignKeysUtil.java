package edu.brown.catalog;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Column;
import org.voltdb.catalog.ColumnRef;
import org.voltdb.catalog.Constraint;
import org.voltdb.catalog.ConstraintRef;
import org.voltdb.catalog.Database;
import org.voltdb.catalog.Table;
import org.voltdb.types.ConstraintType;

import edu.brown.utils.ProjectType;

public abstract class ForeignKeysUtil {
    /** java.util.logging logger. */
    private static final Logger LOG = Logger.getLogger(ForeignKeysUtil.class.getName());

    //
    // A counter for when we create foreign key constraints
    //
    protected static Integer FKEY_COUNTER = 0;

    public static class ForeignKeyMapping extends HashMap<String, String> {
        private static final long serialVersionUID = -8801876844088553783L;

        //
        // Nothing for now...
        //
    }

    //
    // TPC-C Foreign Keys
    //
    public static final Map<String, ForeignKeyMapping> TPCC_FOREIGN_KEYS = new HashMap<String, ForeignKeyMapping>();
    static {
        TPCC_FOREIGN_KEYS.put("DISTRICT", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("DISTRICT").put("D_W_ID", "WAREHOUSE.W_ID");

        TPCC_FOREIGN_KEYS.put("CUSTOMER", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("CUSTOMER").put("C_D_ID", "DISTRICT.D_ID");
        TPCC_FOREIGN_KEYS.get("CUSTOMER").put("C_W_ID", "DISTRICT.D_W_ID");

        TPCC_FOREIGN_KEYS.put("HISTORY", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("HISTORY").put("H_D_ID", "DISTRICT.D_ID");
        TPCC_FOREIGN_KEYS.get("HISTORY").put("H_W_ID", "DISTRICT.D_W_ID");
        TPCC_FOREIGN_KEYS.get("HISTORY").put("H_C_D_ID", "CUSTOMER.C_D_ID");
        TPCC_FOREIGN_KEYS.get("HISTORY").put("H_C_W_ID", "CUSTOMER.C_W_ID");
        TPCC_FOREIGN_KEYS.get("HISTORY").put("H_C_ID", "CUSTOMER.C_ID");

        TPCC_FOREIGN_KEYS.put("STOCK", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("STOCK").put("S_I_ID", "ITEM.I_ID");
        TPCC_FOREIGN_KEYS.get("STOCK").put("S_W_ID", "WAREHOUSE.W_ID");

        TPCC_FOREIGN_KEYS.put("ORDERS", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("ORDERS").put("O_D_ID", "CUSTOMER.C_D_ID");
        TPCC_FOREIGN_KEYS.get("ORDERS").put("O_W_ID", "CUSTOMER.C_W_ID");
        TPCC_FOREIGN_KEYS.get("ORDERS").put("O_C_ID", "CUSTOMER.C_ID");

        TPCC_FOREIGN_KEYS.put("NEW_ORDER", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("NEW_ORDER").put("NO_D_ID", "ORDERS.O_D_ID");
        TPCC_FOREIGN_KEYS.get("NEW_ORDER").put("NO_W_ID", "ORDERS.O_W_ID");
        TPCC_FOREIGN_KEYS.get("NEW_ORDER").put("NO_O_ID", "ORDERS.O_ID");

        TPCC_FOREIGN_KEYS.put("ORDER_LINE", new ForeignKeyMapping());
        TPCC_FOREIGN_KEYS.get("ORDER_LINE").put("OL_D_ID", "ORDERS.O_D_ID");
        TPCC_FOREIGN_KEYS.get("ORDER_LINE").put("OL_W_ID", "ORDERS.O_W_ID");
        TPCC_FOREIGN_KEYS.get("ORDER_LINE").put("OL_O_ID", "ORDERS.O_ID");
        TPCC_FOREIGN_KEYS.get("ORDER_LINE").put("OL_SUPPLY_W_ID", "STOCK.S_W_ID");
        TPCC_FOREIGN_KEYS.get("ORDER_LINE").put("OL_I_ID", "STOCK.S_I_ID");
    } // STATIC

    //
    // TPC-E Foreign Keys
    //
    public static final Map<String, ForeignKeyMapping> TPCE_FOREIGN_KEYS = new HashMap<String, ForeignKeyMapping>();
    static {
        TPCE_FOREIGN_KEYS.put("CUSTOMER", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("CUSTOMER").put("C_ST_ID", "STATUS_TYPE.ST_ID");
        TPCE_FOREIGN_KEYS.get("CUSTOMER").put("C_AD_ID", "ADDRESS.AD_ID");

        TPCE_FOREIGN_KEYS.put("CUSTOMER_ACCOUNT", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("CUSTOMER_ACCOUNT").put("CA_B_ID", "BROKER.B_ID");
        TPCE_FOREIGN_KEYS.get("CUSTOMER_ACCOUNT").put("CA_C_ID", "CUSTOMER.C_ID");

        TPCE_FOREIGN_KEYS.put("CUSTOMER_TAXRATE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("CUSTOMER_TAXRATE").put("CX_TX_ID", "TAXRATE.TX_ID");
        TPCE_FOREIGN_KEYS.get("CUSTOMER_TAXRATE").put("CX_C_ID", "CUSTOMER.C_ID");

        TPCE_FOREIGN_KEYS.put("HOLDING", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("HOLDING").put("H_T_ID", "TRADE.T_ID");
        TPCE_FOREIGN_KEYS.get("HOLDING").put("H_CA_ID", "HOLDING_SUMMARY.HS_CA_ID");
        TPCE_FOREIGN_KEYS.get("HOLDING").put("H_S_SYMB", "HOLDING_SUMMARY.HS_S_SYMB");

        TPCE_FOREIGN_KEYS.put("HOLDING_HISTORY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("HOLDING_HISTORY").put("HH_H_T_ID", "TRADE.T_ID");
        TPCE_FOREIGN_KEYS.get("HOLDING_HISTORY").put("HH_T_ID", "TRADE.T_ID");

        TPCE_FOREIGN_KEYS.put("HOLDING_SUMMARY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("HOLDING_SUMMARY").put("HS_CA_ID", "CUSTOMER_ACCOUNT.CA_ID");
        TPCE_FOREIGN_KEYS.get("HOLDING_SUMMARY").put("HS_S_SYMB", "SECURITY.S_SYMB");

        TPCE_FOREIGN_KEYS.put("WATCH_ITEM", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("WATCH_ITEM").put("WI_WL_ID", "WATCH_LIST.WL_ID");
        TPCE_FOREIGN_KEYS.get("WATCH_ITEM").put("WI_S_SYMB", "SECURITY.S_SYMB");

        TPCE_FOREIGN_KEYS.put("WATCH_LIST", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("WATCH_LIST").put("WL_C_ID", "CUSTOMER.C_ID");

        TPCE_FOREIGN_KEYS.put("BROKER", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("BROKER").put("B_ST_ID", "STATUS_TYPE.ST_ID");

        TPCE_FOREIGN_KEYS.put("CASH_TRANSACTION", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("CASH_TRANSACTION").put("CT_T_ID", "TRADE.T_ID");

        TPCE_FOREIGN_KEYS.put("CHARGE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("CHARGE").put("CH_TT_ID", "TRADE_TYPE.TT_ID");

        TPCE_FOREIGN_KEYS.put("COMMISSION_RATE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("COMMISSION_RATE").put("CR_TT_ID", "TRADE_TYPE.TT_ID");
        TPCE_FOREIGN_KEYS.get("COMMISSION_RATE").put("CR_EX_ID", "EXCHANGE.EX_ID");

        TPCE_FOREIGN_KEYS.put("SETTLEMENT", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("SETTLEMENT").put("SE_T_ID", "TRADE.T_ID");

        TPCE_FOREIGN_KEYS.put("TRADE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("TRADE").put("T_ST_ID", "STATUS_TYPE.ST_ID");
        TPCE_FOREIGN_KEYS.get("TRADE").put("T_TT_ID", "TRADE_TYPE.TT_ID");
        TPCE_FOREIGN_KEYS.get("TRADE").put("T_S_SYMB", "SECURITY.S_SYMB");
        TPCE_FOREIGN_KEYS.get("TRADE").put("T_CA_ID", "CUSTOMER_ACCOUNT.CA_ID");

        TPCE_FOREIGN_KEYS.put("TRADE_HISTORY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("TRADE_HISTORY").put("TH_T_ID", "TRADE.T_ID");
        TPCE_FOREIGN_KEYS.get("TRADE_HISTORY").put("TH_ST_ID", "STATUS_TYPE.ST_ID");

        TPCE_FOREIGN_KEYS.put("TRADE_REQUEST", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("TRADE_REQUEST").put("TR_T_ID", "TRADE.T_ID");
        TPCE_FOREIGN_KEYS.get("TRADE_REQUEST").put("TR_TT_ID", "TRADE_TYPE.TT_ID");
        TPCE_FOREIGN_KEYS.get("TRADE_REQUEST").put("TR_S_SYMB", "SECURITY.S_SYMB");
        TPCE_FOREIGN_KEYS.get("TRADE_REQUEST").put("TR_CA_ID", "CUSTOMER_ACCOUNT.CA_ID");
        TPCE_FOREIGN_KEYS.get("TRADE_REQUEST").put("TR_B_ID", "BROKER.B_ID");

        TPCE_FOREIGN_KEYS.put("COMPANY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("COMPANY").put("CO_ST_ID", "STATUS_TYPE.ST_ID");
        TPCE_FOREIGN_KEYS.get("COMPANY").put("CO_IN_ID", "INDUSTRY.IN_ID");
        TPCE_FOREIGN_KEYS.get("COMPANY").put("CO_AD_ID", "ADDRESS.AD_ID");

        TPCE_FOREIGN_KEYS.put("COMPANY_COMPETITOR", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("COMPANY_COMPETITOR").put("CP_CO_ID", "COMPANY.CO_ID");
        TPCE_FOREIGN_KEYS.get("COMPANY_COMPETITOR").put("CP_COMP_CO_ID", "COMPANY.CO_ID");
        TPCE_FOREIGN_KEYS.get("COMPANY_COMPETITOR").put("CP_IN_ID", "INDUSTRY.IN_ID");

        TPCE_FOREIGN_KEYS.put("DAILY_MARKET", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("DAILY_MARKET").put("DM_S_SYMB", "SECURITY.S_SYMB");

        TPCE_FOREIGN_KEYS.put("EXCHANGE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("EXCHANGE").put("EX_AD_ID", "ADDRESS.AD_ID");

        TPCE_FOREIGN_KEYS.put("FINANCIAL", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("FINANCIAL").put("FI_CO_ID", "COMPANY.CO_ID");

        TPCE_FOREIGN_KEYS.put("INDUSTRY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("INDUSTRY").put("IN_SC_ID", "SECTOR.SC_ID");

        TPCE_FOREIGN_KEYS.put("LAST_TRADE", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("LAST_TRADE").put("LT_S_SYMB", "SECURITY.S_SYMB");

        TPCE_FOREIGN_KEYS.put("NEWS_XREF", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("NEWS_XREF").put("NX_NI_ID", "NEWS_ITEM.NI_ID");
        TPCE_FOREIGN_KEYS.get("NEWS_XREF").put("NX_CO_ID", "COMPANY.CO_ID");

        TPCE_FOREIGN_KEYS.put("SECURITY", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("SECURITY").put("S_ST_ID", "STATUS_TYPE.ST_ID");
        TPCE_FOREIGN_KEYS.get("SECURITY").put("S_EX_ID", "EXCHANGE.EX_ID");
        TPCE_FOREIGN_KEYS.get("SECURITY").put("S_CO_ID", "COMPANY.CO_ID");

        TPCE_FOREIGN_KEYS.put("ADDRESS", new ForeignKeyMapping());
        TPCE_FOREIGN_KEYS.get("ADDRESS").put("AD_ZC_CODE", "ZIP_CODE.ZC_CODE");
    } // STATIC

    //
    // Auction Foreign Keys
    //
    // public static final Map<String, ForeignKeyMapping> AUCTION_FOREIGN_KEYS =
    // new HashMap<String, ForeignKeyMapping>();
    // static {
    // AUCTION_FOREIGN_KEYS.put("ITEM", new ForeignKeyMapping());
    // AUCTION_FOREIGN_KEYS.get("ITEM").put("SELLERID", "USER.USERID");
    // AUCTION_FOREIGN_KEYS.get("ITEM").put("CATEGORYID", "CATEGORY.CATID");
    //
    // // AUCTION_FOREIGN_KEYS.put("AUCTION", new ForeignKeyMapping());
    // // AUCTION_FOREIGN_KEYS.get("AUCTION").put("ITEMID", "ITEM.ITEMID");
    // // AUCTION_FOREIGN_KEYS.get("AUCTION").put("HIGHBIDID", "BID.BIDID");
    //
    // AUCTION_FOREIGN_KEYS.put("BID", new ForeignKeyMapping());
    // AUCTION_FOREIGN_KEYS.get("BID").put("AUCTIONID", "AUCTION.AUCTIONID");
    // AUCTION_FOREIGN_KEYS.get("BID").put("BIDDERID", "USER.USERID");
    // } // STATIC

    //
    // TM1 Foreign Keys
    //
    public static final Map<String, ForeignKeyMapping> TM1_FOREIGN_KEYS = new HashMap<String, ForeignKeyMapping>();
    static {
        TM1_FOREIGN_KEYS.put("ACCESS_INFO", new ForeignKeyMapping());
        TM1_FOREIGN_KEYS.get("ACCESS_INFO").put("S_ID", "SUBSCRIBER.S_ID");

        TM1_FOREIGN_KEYS.put("SPECIAL_FACILITY", new ForeignKeyMapping());
        TM1_FOREIGN_KEYS.get("SPECIAL_FACILITY").put("S_ID", "SUBSCRIBER.S_ID");

        TM1_FOREIGN_KEYS.put("CALL_FORWARDING", new ForeignKeyMapping());
        TM1_FOREIGN_KEYS.get("CALL_FORWARDING").put("S_ID", "SPECIAL_FACILITY.S_ID");
        TM1_FOREIGN_KEYS.get("CALL_FORWARDING").put("SF_TYPE", "SPECIAL_FACILITY.SF_TYPE");

    } // STATIC

    //
    // Mapping to foreign key lists
    // Nasty I know, but what else can I do?
    //
    public static final Map<ProjectType, Map<String, ForeignKeyMapping>> FOREIGN_KEYS = new HashMap<ProjectType, Map<String, ForeignKeyMapping>>();
    static {
        FOREIGN_KEYS.put(ProjectType.TPCC, TPCC_FOREIGN_KEYS);
        FOREIGN_KEYS.put(ProjectType.TPCE, TPCE_FOREIGN_KEYS);
        // FOREIGN_KEYS.put(ProjectType.AUCTION, AUCTION_FOREIGN_KEYS);
        FOREIGN_KEYS.put(ProjectType.TM1, TM1_FOREIGN_KEYS);
    } // STATIC

    public static void populateCatalog(Database catalog_db, Map<String, ForeignKeyMapping> mapping) throws Exception {
        for (String table : mapping.keySet()) {
            LOG.info("Setting foreign key dependencies on table '" + table + "'");
            Table catalog_table = catalog_db.getTables().get(table);
            if (catalog_table == null) {
                throw new Exception("ERROR: Table '" + table + " is not in the catalog");
            }
            ForeignKeysUtil.setForeignKeyConstraints(catalog_table, mapping.get(table));
        } // FOR
        return;
    }

    public static void setForeignKeyConstraints(Table catalog_tbl, Map<String, String> fkeys) throws Exception {
        final Database catalog_db = (Database) catalog_tbl.getParent();

        Map<Table, Constraint> table_const_map = new HashMap<Table, Constraint>();
        for (Entry<String, String> entry : fkeys.entrySet()) {
            String column = entry.getKey();
            String fkey[] = entry.getValue().split("\\.");
            Column catalog_col = catalog_tbl.getColumns().get(column);
            Table catalog_fkey_tbl = catalog_db.getTables().get(fkey[0]);
            Column catalog_fkey_col = catalog_fkey_tbl.getColumns().get(fkey[1]);

            if (catalog_fkey_tbl == null) {
                throw new Exception("ERROR: The foreign key table for '" + fkey[0] + "." + fkey[1] + "' is null");
            } else if (catalog_fkey_col == null) {
                throw new Exception("ERROR: The foreign key column for '" + fkey[0] + "." + fkey[1] + "' is null");
            }

            Constraint catalog_const = null;
            if (table_const_map.containsKey(catalog_fkey_tbl)) {
                catalog_const = table_const_map.get(catalog_fkey_tbl);
            } else {
                String fkey_name = "SYS_FK_" + FKEY_COUNTER++;
                catalog_const = catalog_tbl.getConstraints().get(fkey_name);
                if (catalog_const == null) {
                    catalog_const = catalog_tbl.getConstraints().add(fkey_name);
                    catalog_const.setType(ConstraintType.FOREIGN_KEY.getValue());
                    catalog_const.setForeignkeytable(catalog_fkey_tbl);
                } else {
                    LOG.info("Foreign Key '" + fkey_name + "' already exists for table '" + catalog_tbl.getName() + "'. Skipping...");
                    continue;
                }
            }

            //
            // Add the parent ColumnRef to the Constraint
            // The name of the column ref is the name of the column in this
            // table, which then points
            // to the column in the foreign table
            //
            ColumnRef catalog_fkey_col_ref = catalog_const.getForeignkeycols().add(catalog_col.getName());
            catalog_fkey_col_ref.setColumn(catalog_fkey_col);

            //
            // Add the ConstraintRef to the child Column
            //
            ConstraintRef catalog_const_ref = catalog_col.getConstraints().add(catalog_const.getName());
            catalog_const_ref.setConstraint(catalog_const);

            LOG.debug("Added foreign key constraint from '" + catalog_tbl.getName() + "." + column + "' to '" + fkey[0] + "." + fkey[1] + "'");
            table_const_map.put(catalog_fkey_tbl, catalog_const);
        } // FOR
        return;
    }
}
