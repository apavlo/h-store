/***************************************************************************
 *  Copyright (C) 2009 by H-Store Project                                  *
 *  Brown University                                                       *
 *  Massachusetts Institute of Technology                                  *
 *  Yale University                                                        *
 *                                                                         *
 *  Andy Pavlo (pavlo@cs.brown.edu)                                        *
 *  http://www.cs.brown.edu/~pavlo/                                        *
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
package edu.brown.benchmark.tpce;

import java.io.File;
import java.util.Date;

import org.apache.log4j.Logger;
import org.voltdb.types.TimestampType;

public class EGenClientDriver {
    private static final Logger LOG = Logger.getLogger(EGenClientDriver.class.getName());

    /**
     * Initialize the native object
     * 
     * @param configuredCustomerCount
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
//    public native long initialize(String data_path, int configuredCustomerCount, int totalCustomerCount, int scaleFactor, int initialDays);

/*    private native Object[] egenBrokerVolume(long driver_ptr);

    private native Object[] egenCustomerPosition(long driver_ptr);

    private native Object[] egenDataMaintenance(long driver_ptr);

    private native Object[] egenMarketFeed(long driver_ptr);

    private native Object[] egenMarketWatch(long driver_ptr);

    private native Object[] egenSecurityDetail(long driver_ptr);

    private native Object[] egenTradeCleanup(long driver_ptr);

    private native Object[] egenTradeLookup(long driver_ptr);

    private native Object[] egenTradeOrder(long driver_ptr);

    private native Object[] egenTradeResult(long driver_ptr);

    private native Object[] egenTradeStatus(long driver_ptr);

    private native Object[] egenTradeUpdate(long driver_ptr);*/

    private ClientDriver driver_ptr;

    /**
     * Constructor
     * 
     * @param egenloader_path
     * @param totalCustomerCount
     * @param scaleFactor
     * @param initialDays
     */
    public EGenClientDriver(String egenloader_path, int totalCustomerCount, int scaleFactor, int initialDays) {
        assert (egenloader_path != null) : "The EGENLOADER_PATH parameter is null";
        assert (!egenloader_path.isEmpty()) : "The EGENLOADER_PATH parameter is empty";

        
	File library_path = new File(egenloader_path + File.separator + "lib" + File.separator + "libegen.so");
        LOG.debug("Loading in " + EGenClientDriver.class.getSimpleName() + " library '" + library_path + "'");
        
	
/*try {
            System.load(library_path.getAbsolutePath());
        } catch (Exception ex) {
            ex.printStackTrace();
            LOG.fatal("Failed to load " + EGenClientDriver.class.getSimpleName() + " library", ex);
            System.exit(1);
        }*/

        String input_path = new File(egenloader_path + File.separator).getAbsolutePath();
        LOG.debug("Invoking initialization method on driver using data path '" + input_path + "'");
        driver_ptr = new ClientDriver(input_path, totalCustomerCount, totalCustomerCount, scaleFactor, initialDays);
    }

    private Object[] cleanParams(Object[] orig) {
        // We need to switch java.util.Dates to the stupid volt TimestampType
        for (int i = 0; i < orig.length; i++) {
            if (orig[i] instanceof Date) {
                orig[i] = new TimestampType(((Date) orig[i]).getTime());
            }
        } // FOR
        return (orig);
    }

    public Object[] getBrokerVolumeParams() {
    	Object[] obj = driver_ptr.generateBrokerVolumeInput().InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 119: " + obj[1]);
        return (this.cleanParams(obj));
    }

    public Object[] getCustomerPositionParams() {
    	Object[] obj = driver_ptr.generateCustomerPositionInput().InputParameters().toArray();
    	
System.out.println("EGenClientDriver: line: 123: acct_id_idx: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 124: cust_id: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 125: get_history: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 126: tax_id: " + obj[3]);
    	return (this.cleanParams(obj));
//        return (this.cleanParams(driver_ptr.generateCustomerPositionInput().InputParameters().toArray()));
    }

    public Object[] getDataMaintenanceParams() {
        return (this.cleanParams(driver_ptr.generateDataMaintenanceInput().InputParameters().toArray()));
    }

/*    public Object[] getMarketFeedParams() {
        return (this.cleanParams(driver_ptr.generateMarketFeedInput().InputParameters().toArray()));
    }
*/
    public Object[] getMarketWatchParams() {
    	Object[] obj = driver_ptr.generateMarketWatchInput().InputParameters().toArray();
System.out.println("EGenClientDriver: line: 144: acct_id: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 145: c_id: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 146: ending_co_id: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 147: starting_co_id: " + obj[3].toString());
System.out.println("EGenClientDriver: line: 149: industry_name: " + obj[4].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getSecurityDetailParams() {
    	Object[] obj = driver_ptr.generateSecurityDetailInput().InputParameters().toArray();
System.out.println("EGenClientDriver: line: 154: max_rows_to_return: " + obj[0].toString());
System.out.println("EGenClientDriver: line: 155: access_lob_flag: " + obj[1].toString());
System.out.println("EGenClientDriver: line: 156: start_day: " + obj[2].toString());
System.out.println("EGenClientDriver: line: 157: symbol: " + obj[3].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeCleanupParams() {
        return (this.cleanParams(driver_ptr.generateTradeCleanupInput().InputParameters().toArray()));
    }

    public Object[] getTradeLookupParams() {
    	Object[] obj = driver_ptr.generateTradeLookupInput().InputParameters().toArray();
System.out.println("EGenClientDriver: line: 167: trade_id: " + obj[0]);
System.out.println("EGenClientDriver: line: 168: acct_id: " + obj[1]);
System.out.println("EGenClientDriver: line: 169: max_acct_id: " + obj[2]);
System.out.println("EGenClientDriver: line: 170: frame_to_execute: " + obj[3]);
System.out.println("EGenClientDriver: line: 171: max_trades: " + obj[4]);
System.out.println("EGenClientDriver: line: 172: end_trade_dts: " + obj[5].toString());
System.out.println("EGenClientDriver: line: 173: start_trade_dts: " + obj[6].toString());
System.out.println("EGenClientDriver: line: 174: symbol: " + obj[7].toString());
        return (this.cleanParams(obj));
    }

    public Object[] getTradeOrderParams() {
    	int   iTradeType = 0;
        boolean    bExecutorIsAccountOwner = true;
        Object[] obj = driver_ptr.generateTradeOrderInput(iTradeType, bExecutorIsAccountOwner).InputParameters().toArray();
        System.out.println("EGenClientDriver: line: 182: requested_price: " + obj[0]);
        System.out.println("EGenClientDriver: line: 183: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 184: is_lifo: " + obj[2]);
        System.out.println("EGenClientDriver: line: 185: roll_it_back: " + obj[3]);
        System.out.println("EGenClientDriver: line: 186: trade_qty: " + obj[4]);
        System.out.println("EGenClientDriver: line: 187: type_is_margin: " + obj[5]);
        System.out.println("EGenClientDriver: line: 188: co_name: " + obj[6]);
        System.out.println("EGenClientDriver: line: 189: exec_f_name: " + obj[7]);
        System.out.println("EGenClientDriver: line: 190: exec_l_name: " + obj[8]);
        System.out.println("EGenClientDriver: line: 191: exec_tax_id: " + obj[9]);
        System.out.println("EGenClientDriver: line: 192: issue: " + obj[10]);
        System.out.println("EGenClientDriver: line: 193: st_pending_id: " + obj[11]);
        System.out.println("EGenClientDriver: line: 194: st_submitted_id: " + obj[12]);
        System.out.println("EGenClientDriver: line: 195: symbol: " + obj[13]);
        System.out.println("EGenClientDriver: line: 196: trade_type_id: " + obj[14]);
        return (this.cleanParams(obj));
    }

/*    public Object[] getTradeResultParams() {
        return (this.cleanParams(driver_ptr.generateTradeResultInput().InputParameters().toArray()));
    }
*/
    public Object[] getTradeStatusParams() {
    	Object[] obj = driver_ptr.generateTradeStatusInput().InputParameters().toArray();
System.out.println("EGenClientDriver: line: 206: acct_id: " + obj[0]);
        return (this.cleanParams(obj));
    }

    public Object[] getTradeUpdateParams() {
    	Object[] obj = driver_ptr.generateTradeUpdateInput().InputParameters().toArray();
    	System.out.println("EGenClientDriver: line: 182: trade_id: " + obj[0]);
        System.out.println("EGenClientDriver: line: 183: acct_id: " + obj[1]);
        System.out.println("EGenClientDriver: line: 184: max_acct_id: " + obj[2]);
        System.out.println("EGenClientDriver: line: 185: frame_to_execute: " + obj[3]);
        System.out.println("EGenClientDriver: line: 186: max_trades: " + obj[4]);
        System.out.println("EGenClientDriver: line: 187: max_updates: " + obj[5]);
        System.out.println("EGenClientDriver: line: 188: end_trade_dts: " + obj[6]);
        System.out.println("EGenClientDriver: line: 189: start_trade_dts: " + obj[7]);
        System.out.println("EGenClientDriver: line: 190: symbol: " + obj[8]);
        return (this.cleanParams(obj));
    }
}