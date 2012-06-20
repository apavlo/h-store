/**
 * 
 */
package edu.brown.benchmark.tpce;

import org.voltdb.catalog.Database;

import edu.brown.statistics.AbstractTableStatisticsGenerator;
import edu.brown.utils.ProjectType;

/**
 * TPC-E Intial Table Sizes<br/>
 * Benchmark Specification 2.6.1
 * 
 * @author pavlo
 */
public class TPCETableStatisticsGenerator extends AbstractTableStatisticsGenerator {

    /**
     * TPC-E Specification 2.6.1.8
     */
    public static final long BASE_CUSTOMERS = 5000;
    public static final long BASE_ACCOUNTS = BASE_CUSTOMERS * 5;
    public static final long BASE_TRADES = BASE_CUSTOMERS * 17280;
    public static final long BASE_SETTLEMENTS = BASE_TRADES;
    public static final long BASE_COMPANIES = Math.round(BASE_CUSTOMERS * 0.5);
    public static final long BASE_SECURITY = Math.round(BASE_CUSTOMERS * 0.685);
    public static final long BASE_CUSTOMER_TIERS = 3;

    /**
     * @param catalogDb
     * @param projectType
     * @param scaleFactor
     */
    public TPCETableStatisticsGenerator(Database catalog_db, double scale_factor) {
        super(catalog_db, ProjectType.TPCE, scale_factor);
    }

    @Override
    public void createProfiles() {
        // ----------------------------------------------------------------
        // FIXED TABLES
        // ----------------------------------------------------------------
        this.createFixedTableProfiles();

        // ----------------------------------------------------------------
        // SCALING TABLES
        // ----------------------------------------------------------------
        this.createScalingTableProfiles();

        // ----------------------------------------------------------------
        // GROWING TABLES
        // ----------------------------------------------------------------
        this.createGrowingTableProfiles();
    }

    private void createScalingTableProfiles() {
        TableProfile p = null;

        // CUSTOMER
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, false, BASE_CUSTOMERS);
        this.addTableProfile(p);

        // CUSTOMER_TAXRATE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER_TAXRATE, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 2.0);
        this.addTableProfile(p);

        // CUSTOMER_ACCOUNT
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 5.0);
        this.addTableProfile(p);

        // ACCOUNT_PERMISSION
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, 1.42);
        this.addTableProfile(p);

        // ADDRESS
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_ADDRESS, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, 1.0);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_EXCHANGE, 1.0);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 1.0);
        this.addTableProfile(p);

        // BROKER
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_BROKER, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 0.01);
        this.addTableProfile(p);

        // COMPANY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 0.5);
        this.addTableProfile(p);

        // COMPANY_COMPETITOR
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_COMPANY_COMPETITOR, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, 3.0);
        this.addTableProfile(p);

        // DAILY_MARKET
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_DAILY_MARKET, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SECURITY, 1305.0);
        this.addTableProfile(p);

        // FINANCIAL
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_FINANCIAL, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, 20.0);
        this.addTableProfile(p);

        // HOLDING_SUMMARY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_HOLDING_SUMMARY, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, 9.956);
        this.addTableProfile(p);

        // LAST_TRADE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_LAST_TRADE, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SECURITY, 1.0);
        this.addTableProfile(p);

        // NEWS_ITEM
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_NEWS_ITEM, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, 2.0);
        this.addTableProfile(p);

        // NEWS_XREF
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_NEWS_XREF, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_COMPANY, 2.0);
        this.addTableProfile(p);

        // SECURITY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_SECURITY, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 0.685);
        this.addTableProfile(p);

        // SETTLEMENT
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_SETTLEMENT, false, BASE_SETTLEMENTS);
        this.addTableProfile(p);

        // WATCH_LIST
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_WATCH_LIST, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 1.0);
        this.addTableProfile(p);

        // WATCH_ITEM
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_WATCH_ITEM, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_WATCH_LIST, 100.0);
        this.addTableProfile(p);
    }

    private void createGrowingTableProfiles() {
        TableProfile p = null;

        // CASH_TRANSACTION
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_CASH_TRANSACTION, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SETTLEMENT, 0.92);
        this.addTableProfile(p);

        // HOLDING
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_HOLDING, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SETTLEMENT, 0.051);
        this.addTableProfile(p);

        // HOLDING_HISTORY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_HOLDING_HISTORY, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SETTLEMENT, 1.340);
        this.addTableProfile(p);

        // TRADE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_TRADE, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_CUSTOMER, 17280);
        this.addTableProfile(p);

        // TRADE_HISTORY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_TRADE_HISTORY, false);
        p.addAdditionDependency(this.catalog_db, TPCEConstants.TABLENAME_SETTLEMENT, 2.4);
        this.addTableProfile(p);

        // TRADE_REQUEST
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_TRADE_REQUEST, false);
        this.addTableProfile(p);
    }

    /**
     * 
     */
    private void createFixedTableProfiles() {
        TableProfile p = null;

        // TRADE_TYPE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_TRADE_TYPE, true, 5);
        this.addTableProfile(p);

        // EXCHANGE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_EXCHANGE, true, 4);
        this.addTableProfile(p);

        // INDUSTRY
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_INDUSTRY, true, 102);
        this.addTableProfile(p);

        // SECTOR
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_SECTOR, true, 12);
        this.addTableProfile(p);

        // STATUS_TYPE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_STATUS_TYPE, true, 5);
        this.addTableProfile(p);

        // TAXRATE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_TAXRATE, true, 321);
        this.addTableProfile(p);

        // ZIP_CODE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_ZIP_CODE, true, 14741);
        this.addTableProfile(p);

        // CHARGE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_CHARGE, true, BASE_CUSTOMER_TIERS);
        p.addMultiplicativeDependency(this.catalog_db, TPCEConstants.TABLENAME_TRADE_TYPE, 1.0);
        this.addTableProfile(p);

        // COMMISSION_RATE
        p = new TableProfile(this.catalog_db, TPCEConstants.TABLENAME_COMMISSION_RATE, true, BASE_CUSTOMER_TIERS);
        p.addMultiplicativeDependency(this.catalog_db, TPCEConstants.TABLENAME_TAXRATE, 1.0);
        p.addMultiplicativeDependency(this.catalog_db, TPCEConstants.TABLENAME_EXCHANGE, 1.0);
        p.addMultiplicativeDependency(this.catalog_db, TPCEConstants.TABLENAME_TRADE_TYPE, 1.0);
        this.addTableProfile(p);
    }
}
