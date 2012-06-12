/***************************************************************************
 *  Copyright (C) 2012 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Alex Kalinin (akalinin@cs.brown.edu)                                   *
 *  http://www.cs.brown.edu/~akalinin/                                     *
 *                                                                         *
 *  Permission is hereby granted, free of charge, to any person obtaining  *
 *  a copy of this software and associated documentation files (the        *
 *  "Software"), to deal in the Software without restriction, including    *
 *  without limitation the rights to use, copy, modify, merge, publish,    *
 *  distribute, sublicense, and/or sell copies of the Software, and to     *
 *  permit persons to whom the Software is furnished to do so, subject to  *
 *  the following conditions:                                              *
 *                                                                         *
 *  The above copyright notice and this permission notice shall be         *
 *  included in all copies or substantial portions of the Software.        *
 *                                                                         *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,        *
 *  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF     *
 *  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. *
 *  IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR      *
 *  OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,  *
 *  ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR  *
 *  OTHER DEALINGS IN THE SOFTWARE.                                        *
 ***************************************************************************/

package edu.brown.benchmark.tpce.generators;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.voltdb.catalog.Table;

import edu.brown.benchmark.tpce.TPCEConstants;

public class TPCEGenerator {
    private static final Logger LOG = Logger.getLogger(TPCEGenerator.class);
    
    /*
     * Input Files
     */
    private static final String AREA_FILE        = "AREACODE.txt";
    private static final String CHARGE_FILE      = "CHARGE.txt";
    private static final String COMMRATE_FILE    = "COMMISSION_RATE.txt";
    private static final String COMPANY_FILE     = "COMPANY.txt";
    private static final String COMPANYCOMP_FILE = "COMPANY_COMPETITOR.txt";
    private static final String COMPANYSP_FILE   = "COMPANYSPRATE.txt";
    private static final String EXCHANGE_FILE    = "EXCHANGE.txt";
    private static final String FEMFNAME_FILE    = "FEMALEFIRSTNAME.txt";
    private static final String INDUSTRY_FILE    = "INDUSTRY.txt";
    private static final String LNAME_FILE       = "LASTNAME.txt";
    private static final String MALEFNAME_FILE   = "MALEFIRSTNAME.txt";
    private static final String NONTAXACC_FILE   = "NONTAXABLEACCOUNTNAME.txt";
    private static final String SECTOR_FILE      = "SECTOR.txt";
    private static final String SECURITY_FILE    = "SECURITY.txt";
    private static final String STATUS_FILE      = "STATUS_TYPE.txt";
    private static final String STNAME_FILE      = "STREETNAME.txt";
    private static final String STSUFFIX_FILE    = "STREETSUFFIX.txt";
    private static final String TAXACC_FILE      = "TAXABLEACCOUNTNAME.txt";
    private static final String TAXCOUNTRY_FILE  = "TAXRATESCOUNTRY.txt";
    private static final String TAXDIV_FILE      = "TAXRATESDIVISION.txt";
    private static final String TRADETYPE_FILE   = "TRADE_TYPE.txt";
    private static final String ZIPCODE_FILE     = "ZIP_CODE.txt";
    
    /*
     * File Types
     */
    public enum InputFile {
        AREA,
        CHARGE,
        COMMRATE,
        COMPANY,
        COMPANYCOMP,
        COMPANYSP,
        EXCHANGE,
        FEMFNAME,
        INDUSTRY,
        LNAME,
        MALEFNAME,
        NONTAXACC,
        SECTOR,
        SECURITY,
        STATUS,
        STNAME,
        STSUFFIX,
        TAXACC,
        TAXCOUNTRY,
        TAXDIV,
        TRADETYPE,
        ZIPCODE
    }
    
    /*
     * Input Files handlers
     */
    private final Map<InputFile, InputFileHandler> inputFiles = new HashMap<InputFile, InputFileHandler>();
    
    /*
     * Table generators
     */
    private static final Map<String, Class<? extends TableGenerator>> genClasses = new HashMap<String, Class<? extends TableGenerator>>();
    static {
        // Fixed tables
        genClasses.put(TPCEConstants.TABLENAME_ZIP_CODE, ZipCodeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_CHARGE, ChargeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_EXCHANGE, ExchangeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_COMMISSION_RATE, CommissionRateGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_INDUSTRY, IndustryGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_SECTOR, SectorGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_STATUS_TYPE, StatusTypeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_TRADE_TYPE, TradeTypeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_TAXRATE, TaxRateGenerator.class);
        
        // Scaling tables
        genClasses.put(TPCEConstants.TABLENAME_CUSTOMER, CustomerGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_ADDRESS, AddressGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, CustomerAccountsGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, AccountPermsGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_COMPANY, CompanyGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_COMPANY_COMPETITOR, CompanyCompetitorsGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_CUSTOMER_TAXRATE, CustomerTaxRatesGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_DAILY_MARKET, DailyMarketGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_FINANCIAL, FinancialGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_LAST_TRADE, LastTradeGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_NEWS_ITEM, NewsItemGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_NEWS_XREF, NewsXRefGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_SECURITY, SecurityGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_WATCH_LIST, WatchListGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_WATCH_ITEM, WatchItemsGenerator.class);
        genClasses.put(TPCEConstants.TABLENAME_BROKER, BrokerGenerator.class);
    }
    
/*    private static final Map<String, Class<? extends TableGenerator>> genClassesMixed = new HashMap<String, Class<? extends TableGenerator>>();
    static {
        // Mixed tables
        genClasses.put(TPCEConstants.TABLENAME_ACCOUNT_PERMISSION, null);
        genClasses.put(TPCEConstants.TABLENAME_CUSTOMER_ACCOUNT, null);
    }
*/    
    /*
     * Generator parameters
     */
    private long currentCustomers;
    private long customerStartId;
    private final long total_customers;
    private final int scaling_factor;
    private final int initial_days;
    
    public TPCEGenerator(File inputDir, long total_customers, int scaling_factor, int initial_days) {
        /*
         * Create input file handlers
         */
        inputFiles.put(InputFile.AREA, new WeightedFile(new File(inputDir + File.separator + AREA_FILE)));
        inputFiles.put(InputFile.COMPANYSP, new WeightedFile(new File(inputDir + File.separator + COMPANYSP_FILE)));
        inputFiles.put(InputFile.FEMFNAME, new WeightedFile(new File(inputDir + File.separator + FEMFNAME_FILE)));
        inputFiles.put(InputFile.LNAME, new WeightedFile(new File(inputDir + File.separator + LNAME_FILE)));
        inputFiles.put(InputFile.MALEFNAME, new WeightedFile(new File(inputDir + File.separator + MALEFNAME_FILE)));
        inputFiles.put(InputFile.STNAME, new WeightedFile(new File(inputDir + File.separator + STNAME_FILE)));
        inputFiles.put(InputFile.STSUFFIX, new WeightedFile(new File(inputDir + File.separator + STSUFFIX_FILE)));
        inputFiles.put(InputFile.ZIPCODE, new WeightedFile(new File(inputDir + File.separator + ZIPCODE_FILE)));
        
        inputFiles.put(InputFile.CHARGE, new FlatFile(new File(inputDir + File.separator + CHARGE_FILE)));
        inputFiles.put(InputFile.COMMRATE, new FlatFile(new File(inputDir + File.separator + COMMRATE_FILE)));
        inputFiles.put(InputFile.COMPANY, new FlatFile(new File(inputDir + File.separator + COMPANY_FILE)));
        inputFiles.put(InputFile.COMPANYCOMP, new FlatFile(new File(inputDir + File.separator + COMPANYCOMP_FILE)));
        inputFiles.put(InputFile.EXCHANGE, new FlatFile(new File(inputDir + File.separator + EXCHANGE_FILE)));
        inputFiles.put(InputFile.INDUSTRY, new FlatFile(new File(inputDir + File.separator + INDUSTRY_FILE)));
        inputFiles.put(InputFile.NONTAXACC, new FlatFile(new File(inputDir + File.separator + NONTAXACC_FILE)));
        inputFiles.put(InputFile.SECTOR, new FlatFile(new File(inputDir + File.separator + SECTOR_FILE)));
        inputFiles.put(InputFile.SECURITY, new FlatFile(new File(inputDir + File.separator + SECURITY_FILE)));
        inputFiles.put(InputFile.STATUS, new FlatFile(new File(inputDir + File.separator + STATUS_FILE)));
        inputFiles.put(InputFile.TAXACC, new FlatFile(new File(inputDir + File.separator + TAXACC_FILE)));
        inputFiles.put(InputFile.TRADETYPE, new FlatFile(new File(inputDir + File.separator + TRADETYPE_FILE)));
        
        inputFiles.put(InputFile.TAXCOUNTRY, new ArrayFile(new File(inputDir + File.separator + TAXCOUNTRY_FILE)));
        inputFiles.put(InputFile.TAXDIV, new ArrayFile(new File(inputDir + File.separator + TAXDIV_FILE)));
        
        this.currentCustomers = total_customers;
        this.customerStartId = TPCEConstants.DEFAULT_START_CUSTOMER_ID;
        this.total_customers = total_customers;
        this.scaling_factor = scaling_factor;
        this.initial_days = initial_days;
    }
     
    /**
     * This function changes customer parameters for every subsequent table generator created afterwards.
     * Each generator that depends on the number of customers will use these to generate its tuple. The number
     * of customers MUST be multiple of the load unit size (i.e., 1000) and the customers should start from
     * the beginning of the unit. The same table generators can work in parallel. To create generators with different
     * parameters, this function must be called first and then the generators created.
     * 
     * @param numCustomers The number of customers
     * @param startCustomer The starting customer number
     */
    public void changeSessionParams(long numCustomers, long startCustomer) {
        if (numCustomers <= 0 || numCustomers % TPCEConstants.DEFAULT_LOAD_UNIT != 0) {
            throw new IllegalArgumentException("The number of customers must be a positive integer multiple of the load unit size");
        }
        
        if (startCustomer % TPCEConstants.DEFAULT_LOAD_UNIT != 1) {
            throw new IllegalArgumentException("The customer number must start from the beginning of a unit");
        }
        
        if (startCustomer + numCustomers - 1 > total_customers) {
            throw new IllegalArgumentException("The number of customers for a session must not exceed the total number of customers");
        }
        
        this.currentCustomers = numCustomers;
        this.customerStartId = startCustomer;
    }
    
    public long getCustomersNum() {
        return currentCustomers;
    }
    
    public long getStartCustomer() {
        return customerStartId;
    }
    
    // the number of companies depends on the total number of customers
    public long getCompanyCount(long customersNum) {
        return customersNum / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_COMPANIES_PER_UNIT;
    }
    
    // the number of company competitors depends on the total number of customers
    public long getCompanyCompetitorCount(long customersNum) {
        return customersNum / TPCEConstants.DEFAULT_LOAD_UNIT * TPCEConstants.DEFAULT_COMPANY_COMPETITORS_PER_UNIT;
    }
    
    public void parseInputFiles() {
        for (InputFileHandler file: inputFiles.values()) {
            file.parseFile();
        }
    }
    
    public TableGenerator getTableGen(String tableName, Table catalogTable) {
        TableGenerator gen = null;
        
        try {
            Constructor<?> ctor = genClasses.get(tableName).getDeclaredConstructor(Table.class, TPCEGenerator.class);
            gen = (TableGenerator)ctor.newInstance(catalogTable, this);
        }
        catch (NoSuchMethodException e) {
            LOG.error("Cannot create a generator for: '" + tableName + "' -- no constructor");
            System.exit(1);
        }
        catch (InvocationTargetException e) {
            LOG.error("Cannot create a generator for: '" + tableName + "' -- constructor call error");
            System.exit(1);
        }
        catch (IllegalAccessException e) {
            LOG.error("Cannot create a generator for: '" + tableName + "'");
            System.exit(1);
        }
        catch (InstantiationException e) {
            LOG.error("Cannot create a generator for: '" + tableName + "'");
            System.exit(1);
        }
        
        return gen;
    }
    
    public InputFileHandler getInputFile(InputFile file) {
        return inputFiles.get(file);
    }
    
    public long getTotalCustomers() {
        return total_customers;
    }
    
    public int getScalingFactor() {
        return scaling_factor;
    }
    
    public int getInitTradeDays() {
        return initial_days;
    }
}
